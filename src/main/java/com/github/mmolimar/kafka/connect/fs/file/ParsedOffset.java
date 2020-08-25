package com.github.mmolimar.kafka.connect.fs.file;

public class ParsedOffset implements Offset {
    private final long offset;
    private final long size;

    public ParsedOffset(long offset, long size) {
        this.offset = offset;
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
