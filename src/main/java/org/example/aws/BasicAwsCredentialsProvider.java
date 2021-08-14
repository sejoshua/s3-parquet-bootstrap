package org.example.aws;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.example.exception.AwsS3ClientException;

/**
 * @author Joshua Xing
 */
@AllArgsConstructor
public class BasicAwsCredentialsProvider implements AWSCredentialsProvider {
    private final String accessKey;
    private final String secretKey;

    @Override
    public AWSCredentials getCredentials() {
        if (StringUtils.isEmpty(accessKey) || StringUtils.isEmpty(secretKey)) {
            throw new AwsS3ClientException("Access key or secret is null/empty");
        }
        return new BasicAWSCredentials(accessKey, secretKey);
    }

    @Override
    public void refresh() {

    }
}
