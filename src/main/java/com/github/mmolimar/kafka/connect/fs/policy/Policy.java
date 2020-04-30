package com.github.mmolimar.kafka.connect.fs.policy;

import com.github.mmolimar.kafka.connect.fs.file.FileMetadata;
import com.github.mmolimar.kafka.connect.fs.file.Offset;
import com.github.mmolimar.kafka.connect.fs.file.reader.FileReader;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public interface Policy extends Closeable {

    Iterator<FileMetadata> execute() throws IOException;

    FileReader offer(FileMetadata metadata, Map<String, Object> lastOffset) throws IOException;

    boolean hasEnded();

    List<String> getURIs();

    void interrupt();

    Map<String,Object> buildPartition(FileMetadata metadata);
    Map<String,Object> buildOffset(FileMetadata metadata, long recordOffset);
    SchemaAndValue buildKey(FileMetadata metadata);
    SchemaAndValue buildMetadata(FileMetadata metadata, long offset, boolean isLast);
}
