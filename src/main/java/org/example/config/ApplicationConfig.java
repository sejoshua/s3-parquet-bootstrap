package org.example.config;

import lombok.Getter;

import java.util.Map;

/**
 * @author Joshua Xing
 */
@Getter
public class ApplicationConfig {
    private static final ApplicationConfig INSTANCE = new ApplicationConfig();

    // TODO: these should be sourced from configuration file
    private final String region = "ap-southeast-2";
    private final String accessKey = "";
    private final String secretKey = "";
    private final String bucketName = "candidate-67-s3-bucket";
    private final Map<String, Boolean> fileWithHeaderMap = Map.of("AirbnbListing.csv", true,
            "ausnews.csv", false,
            "netflix_titles.csv", true);
    private final String localDumpFolderName = "tmp";
    private final boolean deleteLocalFile = true;
    private final String filterString = "ellipsis";

    private ApplicationConfig() {}

    public static ApplicationConfig getInstance() {
        return INSTANCE;
    }
}
