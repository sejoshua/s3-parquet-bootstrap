package org.example.aws;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Joshua Xing
 */
@AllArgsConstructor
@Slf4j
public class AwsS3FileClientProvider {
    private static final int SOCKET_TIMEOUT = 10 * 60 * 1_000; // 10 minutes
    private static final int MAX_RETRY = 3;

    private final String region;
    private final String accessKey;
    private final String secretKey;
    private final String bucketName;
    private final String basePrefix;

    public AwsS3FileClient getClient() {
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setProtocol(Protocol.HTTPS);
        clientConfig.setSocketTimeout(SOCKET_TIMEOUT);
        clientConfig.setMaxErrorRetry(MAX_RETRY);

        String s3Endpoint = String.format("https://s3-%s.amazonaws.com", region);
        AwsClientBuilder.EndpointConfiguration endpointConfig =
                new AwsClientBuilder.EndpointConfiguration(s3Endpoint, region);

        AWSCredentialsProvider credentialProvider = new BasicAwsCredentialsProvider(accessKey, secretKey);

        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(endpointConfig)
                .withCredentials(credentialProvider)
                .withClientConfiguration(clientConfig)
                .build();
        return new AwsS3FileClient(s3, bucketName, basePrefix);
    }
}
