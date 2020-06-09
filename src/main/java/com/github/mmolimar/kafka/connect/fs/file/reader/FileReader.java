package com.github.mmolimar.kafka.connect.fs.file.reader;

import com.github.mmolimar.kafka.connect.fs.file.Offset;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.SchemaAndValue;

import java.io.Closeable;
import java.util.Iterator;

public interface FileReader extends Iterator<SchemaAndValue>, Closeable {

    Path getFilePath();

    boolean hasNext();

    SchemaAndValue next();

    void seek(Offset offset);

    Offset currentOffset();
}
