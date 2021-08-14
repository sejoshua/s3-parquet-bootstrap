package org.example.service;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang3.StringUtils;
import org.example.exception.CsvFileConversionException;
import org.example.filter.LineFilter;
import org.example.io.FileClient;
import org.example.io.ParquetExporter;
import org.example.io.impl.DefaultHeaderProvider;
import org.example.model.FilteredData;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * @author Joshua Xing
 */
@AllArgsConstructor
@Slf4j
public class S3CsvFileConversionThread extends Thread {
    private static final String ZIP_EXTENSION = "zip";

    private final FileClient client;
    private final LineFilter filter;
    private final ParquetExporter exporter;
    private final Map<String, Boolean> fileWithHeaderMap;

    @Override
    public void run() {
        try {
            convertS3CsvFiles();
        } catch (CsvFileConversionException e) {
            e.printStackTrace();
        } finally {
            log.info("Exiting S3 CSV file conversion process.");
        }
    }

    private void convertS3CsvFiles() throws CsvFileConversionException {
        List<String> fileKeys = client.list(StringUtils.EMPTY);
        log.info("Files on S3 before conversion: {}", fileKeys);

        for (String key : fileKeys) {
            if (!ZIP_EXTENSION.equals(FilenameUtils.getExtension(key))) {
                // assume all zip files have extension "zip"
                log.info("Skip non-zip file: {}", key);
                continue;
            }

            // need to manually close the reader as ZIP archives have a central structure at the end
            CSVReader reader = null;
            try (ZipInputStream zis = new ZipInputStream(client.getFileInputStream(key))) {
                log.info("[Start] process zip file: {}", key);
                ZipEntry zipEntry;
                int fileCount = 0;
                try {
                    while ((zipEntry = zis.getNextEntry()) != null) {
                        if (!zipEntry.isDirectory()) {
                            String fileName = zipEntry.getName();
                            // use BOMInputStream to skip the leading BOM
                            reader = new CSVReader(new InputStreamReader(new BOMInputStream(zis)));
                            processCsvFile(reader, fileName);
                            fileCount++;
                        }
                        zis.closeEntry();
                    }
                } catch (IOException e) {
                    throw new CsvFileConversionException("Failed to process zip entry in file: " + key, e);
                }
                log.info("[Success] process zip file {} with {} file(s) in it", key, fileCount);
            } catch (IOException e) {
                throw new CsvFileConversionException("Failed to close the zip stream for file: " + key, e);
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e) {
                        log.error("Failed to close the CSV reader.", e);
                    }
                }
            }
        }

        List<String> newFileKeys = client.list(StringUtils.EMPTY);
        log.info("Files on S3 after conversion: {}", newFileKeys);
    }

    private void processCsvFile(CSVReader reader, String fileName) throws CsvFileConversionException {
        boolean hasHeader = fileWithHeaderMap.getOrDefault(fileName, false);
        log.info("[Start] read file: {} (with{} header)", fileName, hasHeader ? StringUtils.EMPTY : "out");

        List<String[]> fullContent;
        try {
            fullContent = reader.readAll();
        } catch (IOException | CsvException e) {
            throw new CsvFileConversionException("Failed to read content from the CSV file: " + fileName, e);
        }
        if (fullContent.isEmpty()) {
            log.warn("File {} is empty", fileName);
            return;
        }
        String[] header = hasHeader ? fullContent.get(0) : new DefaultHeaderProvider(fullContent.get(0).length).getHeader();
        List<String[]> filteredContent = fullContent.stream()
                .skip(hasHeader ? 1L : 0L)
                .filter(line -> Arrays.stream(line).anyMatch(filter))
                .collect(Collectors.toList());
        FilteredData filteredData = new FilteredData(fileName, header, filteredContent);
        try {
            exporter.export(filteredData);
        } catch (IOException e) {
            throw new CsvFileConversionException("Failed to export data to S3.", e);
        }
        log.debug("Header: {}", Arrays.asList(header));
        log.debug("Filtered {} line(s) out of {} line(s) from file {}",
                filteredContent.size(), reader.getLinesRead(), fileName);
        log.debug("Filtered content: {}", filteredContent);

        log.info("[Success] read file: {}", fileName);
    }
}
