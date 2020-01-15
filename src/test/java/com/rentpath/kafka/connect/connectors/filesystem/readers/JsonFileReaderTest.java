package com.rentpath.kafka.connect.connectors.filesystem.readers;

import com.github.mmolimar.kafka.connect.fs.file.Offset;
import com.github.mmolimar.kafka.connect.fs.file.reader.schema.FileSchemaReader;
import com.github.mmolimar.kafka.connect.fs.file.reader.TextFileReader;
import com.github.mmolimar.kafka.connect.fs.file.reader.local.LocalFileReaderTestBase;
import com.github.mmolimar.kafka.connect.fs.file.reader.schema.GithubSchemaReader;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.storage.Converter;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.junit.Assert.assertTrue;

public class JsonFileReaderTest extends LocalFileReaderTestBase {
    private static final String FILE_EXTENSION = "json";
    private static Path schemaFile;

    @BeforeClass
    public static void setUp() throws IOException {
        readerClass = JsonFileReader.class;
        dataFile = createDataFile();
        schemaFile = createSchemaFile();
        readerConfig = new HashMap<String, Object>() {{
            put(JsonFileReader.SCHEMA_READER_CLASS, "com.github.mmolimar.kafka.connect.fs.file.reader.schema.FileSchemaReader");
            put(FileSchemaReader.PATH, schemaFile);
        }};
    }

    private static Path createSchemaFile() throws IOException {
        File schemaFile = File.createTempFile("test-schema-", "." + FILE_EXTENSION);
        try (FileWriter writer = new FileWriter(schemaFile)) {
            String value = String.format("{\"type\":\"struct\", \"fields\":[{\"field\":\"index\",\"type\":\"int64\"},{\"field\":\"value\",\"type\":\"string\"}]}");
            try {
                writer.append(value);
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }
        Path path = new Path(new Path(fsUri), schemaFile.getName());
        fs.moveFromLocalFile(new Path(schemaFile.getAbsolutePath()), path);
        return path;
    }

    private static Path createDataFile() throws IOException {
        File txtFile = File.createTempFile("test-", "." + FILE_EXTENSION);
        try (FileWriter writer = new FileWriter(txtFile)) {
            IntStream.range(0, NUM_RECORDS).forEach(index -> {
                String value = String.format("{\"index\":%d, \"value\":\"%s\"}", index, UUID.randomUUID());
                try {
                    writer.append(value + "\n");
                    OFFSETS_BY_INDEX.put(index, Long.valueOf(index++));
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                }
            });
        }
        Path path = new Path(new Path(fsUri), txtFile.getName());
        fs.moveFromLocalFile(new Path(txtFile.getAbsolutePath()), path);
        return path;
    }

    @Ignore(value = "This test does not apply for json files")
    @Test(expected = IOException.class)
    public void emptyFile() throws Throwable {
        super.emptyFile();
    }

    @Ignore(value = "This test does not apply for json files")
    @Test(expected = IOException.class)
    public void invalidFileFormat() throws Throwable {
        super.invalidFileFormat();
    }

    @Override
    protected Offset getOffset(long offset) {
        return new TextFileReader.TextOffset(offset);
    }

    @Override
    protected void checkData(SchemaAndValue record, long index) {
        Struct recordMap = (Struct) record.value();
        assertTrue(recordMap.getInt64("index").equals(index));
    }

    @Override
    protected String getFileExtension() {
        return FILE_EXTENSION;
    }

}
