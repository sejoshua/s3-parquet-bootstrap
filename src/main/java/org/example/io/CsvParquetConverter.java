package org.example.io;

import lombok.AllArgsConstructor;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.commons.io.FilenameUtils;
import org.example.model.FilteredData;

import java.util.stream.Stream;

/**
 * @author Joshua Xing
 */
@AllArgsConstructor
public class CsvParquetConverter {
    private static final String PARQUET_FILE_EXTENSION = ".parquet";

    private final FilteredData data;

    public String getParquetFileName() {
        String baseName = FilenameUtils.getBaseName(data.getOriginalFileName());
        return baseName + PARQUET_FILE_EXTENSION;
    }

    public Schema getSchema() {
        SchemaBuilder.FieldAssembler<Schema> schemaAssembler = SchemaBuilder
                .record("SampleSchema")
                .namespace("org.example")
                .fields();

        for (String column : data.getHeader()) {
            SchemaBuilder.FieldBuilder<Schema> fieldBuilder = schemaAssembler.name(column);
            // assume all fields are string
            Schema fieldSchema = SchemaBuilder.builder().unionOf().nullType().and().stringType().endUnion();
            fieldBuilder.type(fieldSchema).withDefault(null);
        }

        return schemaAssembler.endRecord();
    }

    public Stream<GenericData.Record> getRows(Schema schema) {
        return data.getFilteredLines().stream().map(row -> getRow(row, data.getHeader(), schema));
    }

    private GenericData.Record getRow(String[] row, String[] header, Schema schema) {
        GenericData.Record record = new GenericData.Record(schema);
        for (int i = 0; i < row.length; ++i) {
            record.put(header[i], row[i]);
        }
        return record;
    }
}
