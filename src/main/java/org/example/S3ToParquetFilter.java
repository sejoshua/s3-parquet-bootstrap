package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.example.aws.AwsS3FileClientProvider;
import org.example.config.ApplicationConfig;
import org.example.filter.LineFilter;
import org.example.filter.impl.WordFilter;
import org.example.io.FileClient;
import org.example.io.ParquetExporter;
import org.example.service.S3CsvFileConversionThread;

@Slf4j
public class S3ToParquetFilter {
    public static void main(String[] args) {
        ApplicationConfig config = ApplicationConfig.getInstance();

        AwsS3FileClientProvider clientProvider = new AwsS3FileClientProvider(config.getRegion(),
                config.getAccessKey(), config.getSecretKey(), config.getBucketName(), StringUtils.EMPTY);
        FileClient client = clientProvider.getClient();
        LineFilter filter = new WordFilter(config.getFilterString());
        ParquetExporter exporter = new ParquetExporter(client, config.getLocalDumpFolderName(), config.isDeleteLocalFile());

        Thread s3CsvFileConversionThread = new S3CsvFileConversionThread(client, filter, exporter, config.getFileWithHeaderMap());
        log.info("Starting S3 CSV file conversion thread.");
        s3CsvFileConversionThread.start();
        log.info("Started S3 CSV file conversion thread.");
    }
}
