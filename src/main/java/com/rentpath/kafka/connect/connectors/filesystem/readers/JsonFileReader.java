package com.rentpath.kafka.connect.connectors.filesystem.readers;

import com.github.mmolimar.kafka.connect.fs.file.Offset;
import com.github.mmolimar.kafka.connect.fs.file.reader.FileReader;
import com.github.mmolimar.kafka.connect.fs.file.reader.TextFileReader;
import com.github.mmolimar.kafka.connect.fs.file.reader.schema.SchemaReader;
import com.github.mmolimar.kafka.connect.fs.util.ReflectionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.mortbay.util.ajax.JSON;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig.FILE_READER_PREFIX;

public class JsonFileReader implements FileReader {
    public static final String SCHEMA_READER_CLASS = FILE_READER_PREFIX + "schema.reader.class";
    private static final String INBOUND_DELETE_KEY = "delete";
    private static final String INBOUND_INDEX_KEY = "index";
    private static final String OUTBOUND_DELETE_KEY = "_DELETE";
    private static final String ID_KEY = "_id";

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
        Object parsed  = JSON.parse(jsonValue);
        // handle directive lines
        if (parsed instanceof Map) {
            Map<String, Object> parsedMap = (Map<String, Object>)parsed;
            if (parsedMap.get(INBOUND_INDEX_KEY) instanceof Map)
                // We have a standard upsert directive. Read the next line and continue.
                jsonValue = ((Struct) reader.next().value()).getString(reader.getSchema().fields().get(0).name());
            else if (parsedMap.get(INBOUND_DELETE_KEY) instanceof Map) {
                // We have a deletion directive
                Map<String, Object> deleteMap = (Map<String, Object>) parsedMap.get(INBOUND_DELETE_KEY);
                jsonValue = String.format("{\"%s\":\"%s\"}", OUTBOUND_DELETE_KEY, deleteMap.get(ID_KEY));
            }
        }
        String wrappedJsonValue = String.format("{\"schema\":%s,\"payload\":%s}", schema, jsonValue);
        return converter.toConnectData(null, wrappedJsonValue.getBytes());
    }

    @Override
    public void seek(Offset offset) throws ConnectException, IllegalArgumentException {
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
