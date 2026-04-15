package io.github.hectorvent.floci.services.kinesis;

import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;

import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@QuarkusTest
class KinesisSdkV2IntegrationTest {

    private static final StaticCredentialsProvider CREDS = StaticCredentialsProvider.create(
            AwsBasicCredentials.create("test", "test"));

    @TestHTTPResource
    URI endpoint;

    @Test
    void createStreamWithAwsSdkV2() {
        AtomicReference<SdkHttpRequest> requestRef = new AtomicReference<>();

        try (KinesisClient client = KinesisClient.builder()
                .endpointOverride(endpoint)
                .region(Region.US_EAST_1)
                .credentialsProvider(CREDS)
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .addExecutionInterceptor(new ExecutionInterceptor() {
                            @Override
                            public SdkHttpRequest modifyHttpRequest(Context.ModifyHttpRequest context,
                                                                    ExecutionAttributes executionAttributes) {
                                requestRef.set(context.httpRequest());
                                return context.httpRequest();
                            }
                        })
                        .build())
                .build()) {

            String streamName = "sdk-v2-kinesis-stream";

            assertDoesNotThrow(() -> client.createStream(CreateStreamRequest.builder()
                    .streamName(streamName)
                    .shardCount(1)
                    .build()));

            var response = assertDoesNotThrow(() -> client.describeStreamSummary(
                    DescribeStreamSummaryRequest.builder()
                            .streamName(streamName)
                            .build()));

            SdkHttpRequest request = requestRef.get();
            assertNotNull(request);
            assertEquals("/", request.encodedPath());
            assertEquals("application/x-amz-cbor-1.1", request.firstMatchingHeader("Content-Type").orElse(null));
            assertEquals("Kinesis_20131202.DescribeStreamSummary",
                    request.firstMatchingHeader("X-Amz-Target").orElse(null));
            assertEquals(streamName, response.streamDescriptionSummary().streamName());
        }
    }
}
