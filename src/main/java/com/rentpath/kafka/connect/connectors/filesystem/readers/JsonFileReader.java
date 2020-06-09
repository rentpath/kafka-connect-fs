package com.rentpath.kafka.connect.connectors.filesystem.readers;

import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import com.github.mmolimar.kafka.connect.fs.file.Offset;
import com.github.mmolimar.kafka.connect.fs.file.reader.FileReader;
import com.github.mmolimar.kafka.connect.fs.file.reader.TextFileReader;
import com.github.mmolimar.kafka.connect.fs.file.reader.schema.SchemaReader;
import com.github.mmolimar.kafka.connect.fs.policy.Policy;
import com.github.mmolimar.kafka.connect.fs.util.ReflectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.ConverterConfig;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig.FILE_READER_PREFIX;

public class JsonFileReader implements FileReader {
    public static final String SCHEMA_READER_CLASS = FILE_READER_PREFIX + "schema.reader.class";

    private JsonConverter converter;
    private TextFileReader reader;
    private String schema;
    private SchemaReader schemaReader;

    public JsonFileReader(FileSystem fs, Path filePath, Map<String, Object> config) throws Throwable {
        reader = new TextFileReader(fs, filePath, config);
        converter = new JsonConverter();

        try {
            String klazz = (String) config.get(SCHEMA_READER_CLASS);
            Class<SchemaReader> schemaReaderClass = (Class<SchemaReader>) Class.forName(klazz);
            if (!SchemaReader.class.isAssignableFrom(schemaReaderClass))
                throw new ConfigException("Schema Reader class " + schemaReaderClass + " does not implement interface " + SchemaReader.class);
            schemaReader = ReflectionUtils.makeSchemaReader(fs, schemaReaderClass, config);
            schema = schemaReader.readSchema();
        } catch (ClassNotFoundException e) {
            throw new ConfigException(SCHEMA_READER_CLASS + " class not found. Got: " +
                    config.get(SCHEMA_READER_CLASS));
        }

        Map<String, Object> converterConf = new HashMap<>();
        converterConf.put(ConverterConfig.TYPE_CONFIG, "value");
        converter.configure(converterConf);
    }

    @Override
    public Path getFilePath() {
        return reader.getFilePath();
    }

    @Override
    public boolean hasNext() {
        return reader.hasNext();
    }

    @Override
    public SchemaAndValue next() {
        SchemaAndValue textSNV = reader.next();
        String jsonValue = ((Struct) textSNV.value()).getString(reader.getSchema().fields().get(0).name());
        String wrappedJsonValue = String.format("{\"schema\":%s,\"payload\":%s}", schema, jsonValue);
        return converter.toConnectData(null, wrappedJsonValue.getBytes());
    }

    @Override
    public void seek(Offset offset) {
        reader.seek(offset);
    }

    @Override
    public Offset currentOffset() {
        return reader.currentOffset();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
