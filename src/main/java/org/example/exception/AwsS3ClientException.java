package org.example.exception;

public class AwsS3ClientException extends RuntimeException {
    public AwsS3ClientException(String message) {
        super(message);
    }

    public AwsS3ClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
