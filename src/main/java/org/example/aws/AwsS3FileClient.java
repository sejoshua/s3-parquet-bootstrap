package org.example.aws;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.example.exception.AwsS3ClientException;
import org.example.io.FileClient;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Joshua Xing
 */
@AllArgsConstructor
@Slf4j
public class AwsS3FileClient implements FileClient {
    private static final int ABBREVIATE_CONTENT_LENGTH = 64;

    private final AmazonS3 client;
    private final String bucketName;
    private final String basePrefix;

    public AwsS3FileClient(AmazonS3 client, String bucketName) {
        this(client, bucketName, StringUtils.EMPTY);
    }

    @Override
    public List<String> list(String path) {
        return listObjects(path).stream().map(S3ObjectSummary::getKey).collect(Collectors.toList());
    }

    @Override
    public void uploadFile(String path, String content) {
        if(content == null) {
            content = StringUtils.EMPTY;
        }
        String fullKey = getFullName(path);
        String shortContent = StringUtils.abbreviateMiddle(content, "...", ABBREVIATE_CONTENT_LENGTH);
        int contentLength = content.length();
        String actionGoal;
        if(contentLength == 0) {
            actionGoal = String.format("upload empty object to s3://%s/%s", bucketName, fullKey);
        } else {
            actionGoal = String.format("upload string of length %d, containing %s to s3://%s/%s",
                    contentLength, shortContent, bucketName, fullKey);
        }
        log.info("[Start] {}", actionGoal);
        try {
            PutObjectResult result = client.putObject(bucketName, fullKey, content);
            log.info("[Success] {}", actionGoal);
        } catch (Throwable e) {
            String errorMsg = String.format("Exception occurred uploading string of length %d as S3 object %s. Abbreviated content: \n%s",
                    contentLength, fullKey, shortContent);
            throw fail(actionGoal, new AwsS3ClientException(errorMsg, e));
        }
    }

    @Override
    public void uploadFile(String path, File file) {
        String fullKey = getFullName(path);
        String actionGoal = String.format("upload file %s to s3://%s/%s", file.getAbsolutePath(), bucketName, fullKey);
        log.info("[Start] {}", actionGoal);
        try {
            PutObjectResult result = client.putObject(bucketName, fullKey, file);
            log.info("[Success] {}", actionGoal);
        } catch (Throwable e) {
            String errorMsg = String.format("Exception occurred uploading file %s as S3 object %s/%s",
                    file.getAbsolutePath(), bucketName, fullKey);
            throw fail(actionGoal, new AwsS3ClientException(errorMsg, e));
        }
    }

    @Override
    public InputStream getFileInputStream(String path) {
        return getObject(path).getObjectContent();
    }

    private List<S3ObjectSummary> listObjects(String path) {
        String fullPrefix = getFullName(path);
        String actionGoal = String.format("list objects s3://%s/%s", bucketName, fullPrefix);
        log.info("[Start] {}", actionGoal);
        try {
            ListObjectsV2Request request = new ListObjectsV2Request()
                    .withBucketName(bucketName)
                    .withPrefix(fullPrefix);
            ListObjectsV2Result listResult;
            List<S3ObjectSummary> ret = new ArrayList<>();
            do {
                listResult = client.listObjectsV2(request);
                ret.addAll(listResult.getObjectSummaries());
                String token = listResult.getNextContinuationToken();
                request.setContinuationToken(token);
            } while(listResult.isTruncated());
            log.info("[Success] {}", actionGoal);
            return ret;
        } catch (Throwable e) {
            String errorMsg = String.format("Exception occurred listing object key %s", fullPrefix);
            throw fail(actionGoal, new AwsS3ClientException(errorMsg, e));
        }
    }

    private S3Object getObject(String key) {
        String fullKey = getFullName(key);
        String actionGoal = String.format("download object from s3://%s/%s", bucketName, fullKey);
        log.info("[Start] {}", actionGoal);
        try {
            S3Object result = client.getObject(bucketName, fullKey);
            log.info("[Success] {}", actionGoal);
            return result;
        } catch (Throwable e) {
            String errorMsg = String.format("Exception occurred getting object %s", fullKey);
            throw fail(actionGoal, new AwsS3ClientException(errorMsg, e));
        }
    }

    private String getFullName(String name) {
        if(name.startsWith(basePrefix)) {
            return name;
        }
        return (basePrefix + '/' + name).replace("//", "/");
    }

    private <E extends Exception> E fail(String goal, E e) {
        log.error("[Failed] " + goal, e);
        return e;
    }
}
