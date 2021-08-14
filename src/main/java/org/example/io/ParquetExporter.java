package org.example.io;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.example.exception.ParquetExportException;
import org.example.model.FilteredData;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * @author Joshua Xing
 */
@AllArgsConstructor
@Slf4j
public class ParquetExporter {
    private final FileClient fileClient;
    private final String localDumpFolderName;
    private final boolean deleteLocalFile;

    public void export(FilteredData data) throws IOException {
        CsvParquetConverter converter = new CsvParquetConverter(data);
        Schema schema = converter.getSchema();
        Stream<GenericData.Record> dataStream = converter.getRows(schema);
        File localFile = saveLocally(schema, dataStream, converter.getParquetFileName());

        fileClient.uploadFile(converter.getParquetFileName(), localFile);

        if(deleteLocalFile) {
            safelyDelete(localFile);
        }
    }

    private File saveLocally(Schema schema, Stream<GenericData.Record> dataStream, String fileName) throws IOException {
        String actionGoal = String.format("[Start] save parquet file locally as %s", fileName);
        log.info("[Start] {}", actionGoal);
        try {
            File rawFile = Paths.get(localDumpFolderName, fileName).toFile();
            File parent = rawFile.getParentFile();
            if (!parent.exists() && parent.mkdir()) {
                log.info("Created {} folder for rate dump files.", parent.getAbsolutePath());
            }
            if (rawFile.exists()) {
                safelyDelete(rawFile);
            }

            Path filePath = new Path(rawFile.getAbsolutePath());
            Configuration config = new Configuration();

            HadoopOutputFile outputFile = HadoopOutputFile.fromPath(filePath, config);
            try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(outputFile)
                    .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                    .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                    .withSchema(schema)
                    .withConf(config)
                    .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                    .withValidation(false)
                    .withDictionaryEncoding(false)
                    .build()) {
                Optional<Exception> failure = dataStream.flatMap(r -> {
                    try {
                        writer.write(r);
                        return Stream.empty();
                    } catch (Exception e) {
                        return Stream.of(e);
                    }
                }).findFirst();
                if (failure.isPresent()) {
                    throw failure.get();
                }
            }
            log.info("[Success] {}", actionGoal);
            return rawFile;
        } catch (Exception e) {
            log.error(String.format("[Failed] %s", actionGoal), e);
            throw new ParquetExportException(String.format("Failed to save %s locally.", fileName), e);
        }
    }

    private void safelyDelete(File toBeDeleted) {
        try {
            if (toBeDeleted != null && !toBeDeleted.delete()) {
                log.warn("Failed to delete file: {}", toBeDeleted.getAbsolutePath());
            }
        } catch (Exception e) {
            log.error("Failed to delete file: " + toBeDeleted.getAbsolutePath(), e);
        }
    }
}
