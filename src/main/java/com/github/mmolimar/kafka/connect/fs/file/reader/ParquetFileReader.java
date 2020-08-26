package com.github.mmolimar.kafka.connect.fs.file.reader;

import com.github.mmolimar.kafka.connect.fs.file.Offset;
import io.confluent.connect.avro.AvroData;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;

import static com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig.FILE_READER_PREFIX;

public class ParquetFileReader extends AbstractFileReader<GenericRecord> {

    private static final String FILE_READER_PARQUET = FILE_READER_PREFIX + "parquet.";

    public static final String FILE_READER_PARQUET_SCHEMA = FILE_READER_PARQUET + "schema";
    public static final String FILE_READER_PARQUET_PROJECTION = FILE_READER_PARQUET + "projection";

    private final ParquetOffset offset;

    private ParquetReader<GenericRecord> reader;
    private GenericRecord currentRecord;
    private Schema schema;
    private Schema projection;
    private boolean closed;


    public ParquetFileReader(FileSystem fs, Path filePath, Map<String, Object> config) throws IOException {
        super(fs, filePath, config);

        this.offset = new ParquetOffset(0);
        this.reader = initReader();
        this.closed = false;
    }

    @Override
    protected ReaderAdapter<GenericRecord> buildAdapter(Map<String, Object> config) {
        return new GenericRecordToStruct();
    }

    private ParquetReader<GenericRecord> initReader() throws IOException {
        Configuration configuration = getFs().getConf();
        if (this.schema != null) {
            AvroReadSupport.setAvroReadSchema(configuration, this.schema);
        }
        if (this.projection != null) {
            AvroReadSupport.setRequestedProjection(configuration, this.projection);
        }
        ParquetReader reader = AvroParquetReader.<GenericRecord>builder(getFilePath())
                .withConf(configuration).build();
        return reader;
    }

    protected void configure(Map<String, Object> config) {
        if (config.get(FILE_READER_PARQUET_SCHEMA) != null) {
            this.schema = new Schema.Parser().parse(config.get(FILE_READER_PARQUET_SCHEMA).toString());
        } else {
            this.schema = null;
        }
        if (config.get(FILE_READER_PARQUET_PROJECTION) != null) {
            this.projection = new Schema.Parser().parse(config.get(FILE_READER_PARQUET_PROJECTION).toString());
        } else {
            this.projection = null;
        }
    }

    @Override
    public boolean hasNext() {
        if (closed) return false;
        if (currentRecord == null) {
            try {
                currentRecord = reader.read();
                if (currentRecord != null) offset.inc();
            } catch (IOException ioe) {
                throw new ConnectException("Error reading parquet record", ioe);
            }
        }
        return currentRecord != null;
    }

    @Override
    protected GenericRecord nextRecord() {
        if (!hasNext()) {
            throw new NoSuchElementException("There are no more records in file: " + getFilePath());
        }
        GenericRecord record;
        if (this.projection != null) {
            record = new GenericData.Record(this.projection);
            this.projection.getFields().forEach(field -> record.put(field.name(), currentRecord.get(field.name())));
        } else {
            record = currentRecord;
        }
        currentRecord = null;
        return record;
    }

    @Override
    public void seek(Offset offset) throws ConnectException, IllegalArgumentException {
        if (closed) {
            throw new ConnectException("Stream is closed!");
        }
        if (offset.getRecordOffset() < 0) {
            throw new IllegalArgumentException("Record offset must be greater than 0");
        }
        if (this.offset.getRecordOffset() > offset.getRecordOffset()) {
            try {
                this.reader = initReader();
                this.offset.setOffset(0);
                this.closed = false;
            } catch (IOException ioe) {
                throw new ConnectException("Error initializing parquet reader", ioe);
            }
        }
        while (hasNext() && this.offset.getRecordOffset() <= offset.getRecordOffset()) {
            nextRecord();
        }
    }

    @Override
    public Offset currentOffset() {
        return offset;
    }

    @Override
    public void close() throws IOException {
        this.closed = true;
        reader.close();
    }

    public static class ParquetOffset implements Offset {
        private long offset;

        public ParquetOffset(long offset) {
            this.offset = offset;
        }

        public void setOffset(long offset) {
            this.offset = offset;
        }

        protected void inc() {
            this.offset++;
        }

        @Override
        public long getRecordOffset() {
            return offset;
        }

        @Override
        public long getRecordOffsetSize() {
            return 1;
        }
    }

    static class GenericRecordToStruct implements ReaderAdapter<GenericRecord> {
        private static final int CACHE_SIZE = 100;
        private final AvroData avroData;

        public GenericRecordToStruct() {
            this.avroData = new AvroData(CACHE_SIZE);
        }

        @Override
        public SchemaAndValue apply(GenericRecord record) {
            return avroData.toConnectData(record.getSchema(), record);
        }
    }
}
