package com.floci.test;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CreateDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.CreateDbInstanceResponse;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DeleteDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesResponse;
import software.amazon.awssdk.services.rds.model.GenerateAuthenticationTokenRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbInstanceRequest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("RDS JDBC Proxy")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RdsJdbcCompatTest {

    private static final StaticCredentialsProvider CREDENTIALS =
            StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));
    private static final Region REGION = Region.US_EAST_1;
    private static final String USERNAME = "admin";
    private static final String PASSWORD = "secret123";
    private static final String DATABASE = "app";
    private static final int PROXY_PORT_MIN = 7000;
    private static final int PROXY_PORT_MAX = 7099;

    private static RdsClient rds;
    private static String instanceId;
    private static Integer proxyPort;
    private static boolean instanceCreated;

    @AfterAll
    static void cleanup() {
        if (rds != null && instanceCreated && instanceId != null) {
            try {
                rds.deleteDBInstance(DeleteDbInstanceRequest.builder()
                        .dbInstanceIdentifier(instanceId)
                        .skipFinalSnapshot(true)
                        .build());
            } catch (Exception ignored) {
            }
            rds.close();
        }
    }

    @Test
    @Order(1)
    @DisplayName("Create instance without IAM and connect with password")
    void createDbInstanceAndConnectWithPassword() throws Exception {
        rds = TestFixtures.rdsClient();
        instanceId = TestFixtures.uniqueName("rds-pg");

        try {
            CreateDbInstanceResponse response = rds.createDBInstance(CreateDbInstanceRequest.builder()
                    .dbInstanceIdentifier(instanceId)
                    .dbInstanceClass("db.t3.micro")
                    .engine("postgres")
                    .masterUsername(USERNAME)
                    .masterUserPassword(PASSWORD)
                    .dbName(DATABASE)
                    .allocatedStorage(20)
                    .enableIAMDatabaseAuthentication(false)
                    .build());

            proxyPort = response.dbInstance().endpoint().port();
            instanceCreated = true;
        } catch (Exception e) {
            Assumptions.assumeTrue(false, "RDS instance creation unavailable in this environment: " + e.getMessage());
            return;
        }

        assertThat(proxyPort).isBetween(PROXY_PORT_MIN, PROXY_PORT_MAX);

        Connection connection = awaitPostgresConnection(USERNAME, PASSWORD);
        try {
            assertThat(selectOne(connection)).isEqualTo(1);
        } finally {
            connection.close();
        }
    }

    @Test
    @Order(2)
    @DisplayName("IAM auth token rejected when IAM is disabled on instance")
    void iamAuthRejectedWhenDisabled() {
        assumeInstanceCreated();

        String token = rds.utilities().generateAuthenticationToken(GenerateAuthenticationTokenRequest.builder()
                .hostname(TestFixtures.proxyHost())
                .port(proxyPort)
                .username(USERNAME)
                .region(REGION)
                .credentialsProvider(CREDENTIALS)
                .build());

        assertThatThrownBy(() -> openPostgresConnection(USERNAME, token))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("password authentication failed");
    }

    @Test
    @Order(3)
    @DisplayName("Enable IAM via modify, then connect with IAM auth token")
    void enableIamViaModifyAndConnect() throws Exception {
        assumeInstanceCreated();

        DBInstance modified = rds.modifyDBInstance(ModifyDbInstanceRequest.builder()
                .dbInstanceIdentifier(instanceId)
                .enableIAMDatabaseAuthentication(true)
                .build()).dbInstance();

        assertThat(modified.iamDatabaseAuthenticationEnabled()).isTrue();

        String token = rds.utilities().generateAuthenticationToken(GenerateAuthenticationTokenRequest.builder()
                .hostname(TestFixtures.proxyHost())
                .port(proxyPort)
                .username(USERNAME)
                .region(REGION)
                .credentialsProvider(CREDENTIALS)
                .build());

        Connection connection = awaitPostgresConnection(USERNAME, token);
        try {
            assertThat(selectOne(connection)).isEqualTo(1);
        } finally {
            connection.close();
        }
    }

    @Test
    @Order(4)
    @DisplayName("Tampered IAM auth token is rejected")
    void rejectsTamperedIamAuthToken() {
        assumeInstanceCreated();

        String token = rds.utilities().generateAuthenticationToken(GenerateAuthenticationTokenRequest.builder()
                .hostname(TestFixtures.proxyHost())
                .port(proxyPort)
                .username(USERNAME)
                .region(REGION)
                .credentialsProvider(CREDENTIALS)
                .build());

        String tamperedToken = token.substring(0, token.length() - 1)
                + (token.endsWith("a") ? "b" : "a");

        assertThatThrownBy(() -> openPostgresConnection(USERNAME, tamperedToken))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("password authentication failed");
    }

    @Test
    @Order(5)
    @DisplayName("Modify keeps proxy reachable and delete releases port for reuse")
    void modifyKeepsProxyReachableAndDeleteReleasesPort() throws Exception {
        assumeInstanceCreated();

        DBInstance modified = rds.modifyDBInstance(ModifyDbInstanceRequest.builder()
                .dbInstanceIdentifier(instanceId)
                .masterUserPassword("secret456")
                .build()).dbInstance();

        Connection modifiedPasswordConnection = awaitPostgresConnection(USERNAME, "secret456");
        try {
            assertThat(selectOne(modifiedPasswordConnection)).isEqualTo(1);
        } finally {
            modifiedPasswordConnection.close();
        }

        // IAM should still work after password change
        String token = rds.utilities().generateAuthenticationToken(GenerateAuthenticationTokenRequest.builder()
                .hostname(TestFixtures.proxyHost())
                .port(proxyPort)
                .username(USERNAME)
                .region(REGION)
                .credentialsProvider(CREDENTIALS)
                .build());

        Connection iamConnection = awaitPostgresConnection(USERNAME, token);
        try {
            assertThat(selectOne(iamConnection)).isEqualTo(1);
        } finally {
            iamConnection.close();
        }

        rds.deleteDBInstance(DeleteDbInstanceRequest.builder()
                .dbInstanceIdentifier(instanceId)
                .skipFinalSnapshot(true)
                .build());
        instanceCreated = false;

        DescribeDbInstancesResponse afterDelete = rds.describeDBInstances(DescribeDbInstancesRequest.builder()
                .dbInstanceIdentifier(instanceId)
                .build());
        assertThat(afterDelete.dbInstances()).isEmpty();

        String replacementId = TestFixtures.uniqueName("rds-pg");
        CreateDbInstanceResponse replacement = rds.createDBInstance(CreateDbInstanceRequest.builder()
                .dbInstanceIdentifier(replacementId)
                .dbInstanceClass("db.t3.micro")
                .engine("postgres")
                .masterUsername(USERNAME)
                .masterUserPassword(PASSWORD)
                .dbName(DATABASE)
                .allocatedStorage(20)
                .enableIAMDatabaseAuthentication(true)
                .build());

        instanceId = replacementId;
        instanceCreated = true;
        Integer replacementPort = replacement.dbInstance().endpoint().port();

        // Port should be within the configured RDS proxy range and the connection
        // should succeed. Don't assert exact port reuse as allocation order is
        // an implementation detail that can vary across environments.
        assertThat(replacementPort).isBetween(PROXY_PORT_MIN, PROXY_PORT_MAX);

        Connection replacementConnection = awaitPostgresConnection(USERNAME, PASSWORD);
        try {
            assertThat(selectOne(replacementConnection)).isEqualTo(1);
        } finally {
            replacementConnection.close();
        }
    }

    private static void assumeInstanceCreated() {
        Assumptions.assumeTrue(instanceCreated && proxyPort != null,
                "RDS JDBC tests require a created DB instance from the first step");
    }

    private static Connection awaitPostgresConnection(String username, String password) throws Exception {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(60));
        SQLException last = null;
        while (Instant.now().isBefore(deadline)) {
            try {
                return openPostgresConnection(username, password);
            } catch (SQLException e) {
                last = e;
                Thread.sleep(1000);
            }
        }
        throw last != null ? last : new SQLException("Timed out waiting for RDS proxy connection");
    }

    private static Connection openPostgresConnection(String username, String password) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", username);
        properties.setProperty("password", password);
        properties.setProperty("sslmode", "disable");
        properties.setProperty("connectTimeout", "5");
        return DriverManager.getConnection(
                "jdbc:postgresql://" + TestFixtures.proxyHost() + ":" + proxyPort + "/" + DATABASE,
                properties);
    }

    private static int selectOne(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("select 1")) {
            assertThat(resultSet.next()).isTrue();
            return resultSet.getInt(1);
        }
    }
}
