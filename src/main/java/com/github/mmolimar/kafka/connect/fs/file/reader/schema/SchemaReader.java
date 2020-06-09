package com.github.mmolimar.kafka.connect.fs.file.reader.schema;

import java.io.IOException;
import java.util.Map;

public interface SchemaReader {
    public String readSchema() throws IOException;
}
