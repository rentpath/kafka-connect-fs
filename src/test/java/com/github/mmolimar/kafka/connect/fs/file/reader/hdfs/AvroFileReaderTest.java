package com.github.mmolimar.kafka.connect.fs.file.reader.hdfs;

import com.github.mmolimar.kafka.connect.fs.file.Offset;
import com.github.mmolimar.kafka.connect.fs.file.reader.AgnosticFileReader;
import com.github.mmolimar.kafka.connect.fs.file.reader.AvroFileReader;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.junit.Assert.assertTrue;

public class AvroFileReaderTest extends HdfsFileReaderTestBase {

    private static final String FIELD_INDEX = "index";
    private static final String FIELD_NAME = "name";
    private static final String FIELD_SURNAME = "surname";
    private static final String FILE_EXTENSION = "avro";

    private static Schema schema;

    @BeforeClass
    public static void setUp() throws IOException {
        schema = new Schema.Parser().parse(AvroFileReaderTest.class.getResourceAsStream("/file/reader/schemas/people.avsc"));
        readerClass = AgnosticFileReader.class;
        dataFile = createDataFile();
        readerConfig = new HashMap<>();
    }

    private static Path createDataFile() throws IOException {
        File avroFile = File.createTempFile("test-", "." + FILE_EXTENSION);
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(writer)) {
            dataFileWriter.setFlushOnEveryBlock(true);
            dataFileWriter.setSyncInterval(32);
            dataFileWriter.create(schema, avroFile);

            IntStream.range(0, NUM_RECORDS).forEach(index -> {
                GenericRecord datum = new GenericData.Record(schema);
                datum.put(FIELD_INDEX, index);
                datum.put(FIELD_NAME, String.format("%d_name_%s", index, UUID.randomUUID()));
                datum.put(FIELD_SURNAME, String.format("%d_surname_%s", index, UUID.randomUUID()));
                try {
                    OFFSETS_BY_INDEX.put(index, dataFileWriter.sync() - 16L);
                    dataFileWriter.append(datum);
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                }
            });
        }
        Path path = new Path(new Path(fsUri), avroFile.getName());
        fs.moveFromLocalFile(new Path(avroFile.getAbsolutePath()), path);
        return path;
    }

    @Test
    public void readerWithSchema() throws Throwable {
        Map<String, Object> cfg = new HashMap<String, Object>() {{
            put(AvroFileReader.FILE_READER_AVRO_SCHEMA, schema.toString());
        }};
        reader = getReader(fs, dataFile, cfg);
        readAllData();
    }

    @Test(expected = AvroTypeException.class)
    public void readerWithInvalidSchema() throws Throwable {
        Map<String, Object> cfg = new HashMap<String, Object>() {{
            put(AvroFileReader.FILE_READER_AVRO_SCHEMA, Schema.create(Schema.Type.STRING).toString());
        }};
        reader = getReader(fs, dataFile, cfg);
        readAllData();
    }

    @Test(expected = SchemaParseException.class)
    public void readerWithUnparseableSchema() throws Throwable {
        Map<String, Object> cfg = new HashMap<String, Object>() {{
            put(AvroFileReader.FILE_READER_AVRO_SCHEMA, "invalid schema");
        }};
        getReader(fs, dataFile, cfg);
    }

    @Override
    protected Offset getOffset(long offset) {
        return new AvroFileReader.AvroOffset(offset);
    }

    @Override
    protected void checkData(SchemaAndValue record, long index) {
        Struct recordStruct = (Struct) record.value();
        assertTrue((Integer) recordStruct.get(FIELD_INDEX) == index);
        assertTrue(recordStruct.get(FIELD_NAME).toString().startsWith(index + "_"));
        assertTrue(recordStruct.get(FIELD_SURNAME).toString().startsWith(index + "_"));
    }

    @Override
    protected String getFileExtension() {
        return FILE_EXTENSION;
    }
}
