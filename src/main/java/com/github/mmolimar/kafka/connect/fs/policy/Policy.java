package com.github.mmolimar.kafka.connect.fs.policy;

import com.github.mmolimar.kafka.connect.fs.file.FileMetadata;
import com.github.mmolimar.kafka.connect.fs.file.Offset;
import com.github.mmolimar.kafka.connect.fs.file.reader.FileReader;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;

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

    Map<String,Object> buildOffsetPartition(FileMetadata metadata);
    Map<String,Object> buildOffset(FileMetadata metadata, FileMetadata exemplarMetadata, Offset offset, Map<String,Object> priorOffset, boolean isLast);
    SchemaAndValue buildKey(FileMetadata metadata, SchemaAndValue snvValue, Map<String, Object> offset);
    SchemaAndValue buildMetadata(FileMetadata metadata, long offset, boolean isLast, Map<String, Object> connectorOffset, String nextRecordPath);
    FileMetadata extractExemplar(List<FileMetadata> batchFileMetadata);
    void seekReader(FileMetadata metadata, Map<String, Object> offset, FileReader reader) throws ConnectException, IOException, IllegalArgumentException;
}
