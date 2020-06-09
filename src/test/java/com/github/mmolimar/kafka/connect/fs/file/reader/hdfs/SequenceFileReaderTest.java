package com.github.mmolimar.kafka.connect.fs.file.reader.hdfs;

import com.github.mmolimar.kafka.connect.fs.file.Offset;
import com.github.mmolimar.kafka.connect.fs.file.reader.AgnosticFileReader;
import com.github.mmolimar.kafka.connect.fs.file.reader.SequenceFileReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SequenceFileReaderTest extends HdfsFileReaderTestBase {

    private static final String FIELD_NAME_KEY = "key";
    private static final String FIELD_NAME_VALUE = "value";
    private static final String FILE_EXTENSION = "seq";

    @BeforeClass
    public static void setUp() throws IOException {
        readerClass = AgnosticFileReader.class;
        dataFile = createDataFile();
        readerConfig = new HashMap<String, Object>() {{
            put(SequenceFileReader.FILE_READER_SEQUENCE_FIELD_NAME_KEY, FIELD_NAME_KEY);
            put(SequenceFileReader.FILE_READER_SEQUENCE_FIELD_NAME_VALUE, FIELD_NAME_VALUE);
        }};
    }

    private static Path createDataFile() throws IOException {
        File seqFile = File.createTempFile("test-", "." + FILE_EXTENSION);
        try (SequenceFile.Writer writer = SequenceFile.createWriter(fs.getConf(), SequenceFile.Writer.file(new Path(seqFile.getAbsolutePath())),
                SequenceFile.Writer.keyClass(IntWritable.class), SequenceFile.Writer.valueClass(Text.class))) {

            IntStream.range(0, NUM_RECORDS).forEach(index -> {
                Writable key = new IntWritable(index);
                Writable value = new Text(String.format("%d_%s", index, UUID.randomUUID()));
                try {
                    writer.append(key, value);
                    writer.sync();
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                }
            });
        }
        try (SequenceFile.Reader reader = new SequenceFile.Reader(fs.getConf(),
                SequenceFile.Reader.file(new Path(seqFile.getAbsolutePath())))) {
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), fs.getConf());
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), fs.getConf());
            int index = 0;
            long pos = reader.getPosition() - 1;
            while (reader.next(key, value)) {
                OFFSETS_BY_INDEX.put(index++, pos);
                pos = reader.getPosition();
            }
        }
        Path path = new Path(new Path(fsUri), seqFile.getName());
        fs.moveFromLocalFile(new Path(seqFile.getAbsolutePath()), path);
        return path;
    }

    @Test
    public void defaultFieldNames() throws Throwable {
        Map<String, Object> customReaderCfg = new HashMap<>();
        reader = getReader(fs, dataFile, customReaderCfg);
        assertTrue(reader.getFilePath().equals(dataFile));

        assertTrue(reader.hasNext());

        int recordCount = 0;
        while (reader.hasNext()) {
            checkData(SequenceFileReader.FIELD_NAME_KEY_DEFAULT, SequenceFileReader.FIELD_NAME_VALUE_DEFAULT, reader.next(), recordCount);
            recordCount++;
        }
        assertEquals("The number of records in the file does not match", NUM_RECORDS, recordCount);
    }

    @Override
    protected Offset getOffset(long offset) {
        return new SequenceFileReader.SeqOffset(offset);
    }

    @Override
    protected void checkData(SchemaAndValue record, long index) {
        checkData(FIELD_NAME_KEY, FIELD_NAME_VALUE, record, index);
    }

    private void checkData(String keyFieldName, String valueFieldName, SchemaAndValue record, long index) {
        Struct recordStruct = (Struct) record.value();
        assertTrue((Integer) recordStruct.get(keyFieldName) == index);
        assertTrue(recordStruct.get(valueFieldName).toString().startsWith(index + "_"));
    }

    @Override
    protected String getFileExtension() {
        return FILE_EXTENSION;
    }

}
