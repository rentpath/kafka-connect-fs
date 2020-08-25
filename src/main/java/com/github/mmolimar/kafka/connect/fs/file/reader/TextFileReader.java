package com.github.mmolimar.kafka.connect.fs.file.reader;

import com.github.mmolimar.kafka.connect.fs.file.Offset;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.NoSuchElementException;

import static com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig.FILE_READER_PREFIX;

public class TextFileReader extends AbstractFileReader<TextFileReader.TextRecord> {

    public static final String FIELD_NAME_VALUE_DEFAULT = "value";

    private static final String FILE_READER_TEXT = FILE_READER_PREFIX + "text.";
    private static final String FILE_READER_SEQUENCE_FIELD_NAME_PREFIX = FILE_READER_TEXT + "field_name.";

    public static final String FILE_READER_TEXT_FIELD_NAME_VALUE = FILE_READER_SEQUENCE_FIELD_NAME_PREFIX + "value";
    public static final String FILE_READER_TEXT_ENCODING = FILE_READER_TEXT + "encoding";

    public static final String FILE_READER_FILES_GZIPPED = FILE_READER_PREFIX + "gzipped";

    private final TextOffset offset;
    private String currentLine;
    private boolean finished = false;
    protected LineNumberReader reader;
    private Schema schema;
    private Charset charset;
    private boolean gzipped;

    public TextFileReader(FileSystem fs, Path filePath, Map<String, Object> config) throws IOException {
        super(fs, filePath, config);
        this.reader = buildReader(fs, filePath, config);
        this.offset = new TextOffset(0);
    }

    protected LineNumberReader buildReader(FileSystem fs, Path filePath, Map<String, Object> config) throws IOException {
        gzipped = Boolean.parseBoolean((String) config.get(FILE_READER_FILES_GZIPPED));
        return buildReader(fs, filePath, gzipped);
    }

    protected LineNumberReader buildReader(FileSystem fs, Path filePath, boolean gzipped) throws IOException {
        if (gzipped)
            return new LineNumberReader(new InputStreamReader(new GzipCompressorInputStream(fs.open(getFilePath()))));
        else
            return new LineNumberReader(new InputStreamReader(fs.open(filePath), this.charset));
    }

    @Override
    protected ReaderAdapter<TextRecord> buildAdapter(Map<String, Object> config) {
        return new TxtToStruct();
    }

    @Override
    protected void configure(Map<String, Object> config) {
        buildSchema(config);
        if (config.get(FILE_READER_TEXT_ENCODING) == null ||
                config.get(FILE_READER_TEXT_ENCODING).toString().equals("")) {
            this.charset = Charset.defaultCharset();
        } else {
            this.charset = Charset.forName(config.get(FILE_READER_TEXT_ENCODING).toString());
        }
    }

    protected void buildSchema(Map<String, Object> config) {
        String valueFieldName;
        if (config.get(FILE_READER_TEXT_FIELD_NAME_VALUE) == null ||
                config.get(FILE_READER_TEXT_FIELD_NAME_VALUE).toString().equals("")) {
            valueFieldName = FIELD_NAME_VALUE_DEFAULT;
        } else {
            valueFieldName = config.get(FILE_READER_TEXT_FIELD_NAME_VALUE).toString();
        }
        this.schema = SchemaBuilder.struct()
                .field(valueFieldName, Schema.STRING_SCHEMA)
                .build();
    }

    @Override
    public boolean hasNext() {
        if (currentLine != null) {
            return true;
        } else if (finished) {
            return false;
        } else {
            try {
                return readNext();
            } catch (IOException ioe) {
                throw new IllegalStateException(ioe);
            }
        }
    }

    protected boolean readNext() throws IOException {
        String line = reader.readLine();
        offset.setOffset(reader.getLineNumber());
        if (line == null) {
            finished = true;
            return false;
        }
        currentLine = line;
        return true;
    }

    @Override
    protected TextRecord nextRecord() {
        if (!hasNext()) {
            throw new NoSuchElementException("There are no more records in file: " + getFilePath());
        }
        String aux = currentLine;
        currentLine = null;

        return new TextRecord(schema, aux);
    }

    @Override
    public void seek(Offset offset) throws ConnectException, IllegalArgumentException {
        if (offset.getRecordOffset() < 0) {
            throw new IllegalArgumentException("Record offset must be greater than 0");
        }
        try {
            int effectiveLineNumber = reader.getLineNumber();
            if (offset.getRecordOffset() < effectiveLineNumber) {
                this.reader = buildReader(getFs(), getFilePath(), gzipped);
                currentLine = null;
            }
            int lineNumber = 0;
            while ((currentLine = reader.readLine()) != null) {
                lineNumber = reader.getLineNumber();
                if (lineNumber - 1 == offset.getRecordOffset()) {
                    // if the size is greater than one, we need to push ahead. Our semantic offset is still the source
                    // line, but the size needs to be accounted for by adding that value to the read lines.
                    for (int i = 0; i < offset.getRecordOffsetSize() - 1; i++)
                        currentLine = reader.readLine();
                    this.offset.setOffset(reader.getLineNumber());
                    this.offset.setSize(offset.getRecordOffsetSize());
                    return;
                }
            }
            this.offset.setOffset(lineNumber);
        } catch (IOException ioe) {
            throw new ConnectException("Error seeking file " + getFilePath(), ioe);
        }
    }

    @Override
    public Offset currentOffset() {
        return offset;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    public Schema getSchema() {
        return schema;
    }

    public static class TextOffset implements Offset {
        private long offset;
        private long size;

        public TextOffset(long offset) {
            this.offset = offset;
            this.size = 1;
        }

        public void setOffset(long offset) {
            this.offset = offset;
        }

        public void setSize(long size) {
            this.size = size;
        }

        @Override
        public long getRecordOffset() {
            return offset;
        }

        @Override
        public long getRecordOffsetSize() {
            return size;
        }
    }

    protected static class TxtToStruct implements ReaderAdapter<TextRecord> {

        @Override
        public SchemaAndValue apply(TextRecord record) {
            Struct struct = new Struct(record.schema)
                    .put(record.schema.fields().get(0), record.value);
            return new SchemaAndValue(struct.schema(), struct);
        }
    }

    protected static class TextRecord {
        public final Schema schema;
        public final String value;

        public TextRecord(Schema schema, String value) {
            this.schema = schema;
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}
