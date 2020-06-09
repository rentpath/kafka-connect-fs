package com.github.mmolimar.kafka.connect.fs.file.reader.schema;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.lf5.util.StreamUtils;
import org.kohsuke.github.GitHubBuilder;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig.FILE_READER_PREFIX;

public class FileSchemaReader implements SchemaReader {
    private static final String SCHEMA_READER_PREFIX = FILE_READER_PREFIX + "schema.reader.";
    public static final String PATH = SCHEMA_READER_PREFIX + "path";

    private Path filepath;
    private FileSystem fs;

    public FileSchemaReader(FileSystem fs, Map<String, Object> config) throws IOException {
        filepath = new Path((String) config.get(PATH));
        this.fs = fs;
    }

    @Override
    public String readSchema() throws IOException {
        return new String(StreamUtils.getBytes(fs.open(filepath)), StandardCharsets.UTF_8);
    }
}
