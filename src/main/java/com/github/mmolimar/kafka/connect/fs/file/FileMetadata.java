package com.github.mmolimar.kafka.connect.fs.file;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileMetadata {
    private String path;
    private long length;
    private Map<String,Object> opts;
    private List<BlockInfo> blocks;

    public FileMetadata(String path, long length, List<BlockInfo> blocks) {
        this.path = path;
        this.length = length;
        this.blocks = blocks;
        this.opts = new HashMap<>();
    }

    public String getPath() {
        return path;
    }

    public long getLen() {
        return length;
    }

    public List<BlockInfo> getBlocks() {
        return blocks;
    }

    public Object getOpt(String key) { return opts.get(key); }

    public void setOpt(String key, Object value) { opts.put(key, value); }

    @Override
    public String toString() {
        return String.format("[path = %s, length = %s, blocks = %s]", path, length, blocks);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (!(object instanceof FileMetadata)) return false;

        FileMetadata metadata = (FileMetadata) object;
        if (this.path.equals(metadata.getPath()) &&
                this.length == metadata.length &&
                this.blocks.equals(metadata.getBlocks())) {
            return true;
        }
        return false;
    }

    public int hashCode() {
        return path == null ? 0 : path.hashCode();
    }


    public static class BlockInfo {
        private long offset;
        private long length;
        private boolean corrupt;

        public BlockInfo(long offset, long length, boolean corrupt) {
            this.offset = offset;
            this.length = length;
            this.corrupt = corrupt;
        }

        @Override
        public String toString() {
            return String.format("[offset = %s, length = %s, corrupt = %s]", offset, length, corrupt);
        }
    }
}