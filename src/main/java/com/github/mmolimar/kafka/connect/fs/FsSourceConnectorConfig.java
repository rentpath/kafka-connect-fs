package com.github.mmolimar.kafka.connect.fs;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.protocol.types.Field;

import java.util.List;
import java.util.Map;


public class FsSourceConnectorConfig extends AbstractConfig {

    public static final String FS_URIS = "fs.uris";
    private static final String FS_URIS_DOC = "Comma-separated URIs of the FS(s).";

    public static final String TOPIC = "topic";
    private static final String TOPIC_DOC = "Topic to copy data to.";

    public static final String POLL_INTERVAL_MS = "poll.interval.ms";
    public static final String POLL_INTERVAL_MS_DOC = "Interval in ms between filesystem polls";

    public static final String INCLUDE_METADATA = "include.metadata";
    public static final String INCLUDE_METADATA_DOC = "Whether or not to include file metadata in the output value";

    public FsSourceConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public FsSourceConnectorConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        return new ConfigDef()
                .define(FS_URIS, Type.LIST, Importance.HIGH, FS_URIS_DOC)
                .define(TOPIC, Type.STRING, Importance.HIGH, TOPIC_DOC)
                .define(POLL_INTERVAL_MS, Type.LONG, 0, Importance.HIGH, POLL_INTERVAL_MS_DOC)
                .define(INCLUDE_METADATA, Type.BOOLEAN, Boolean.FALSE, Importance.HIGH, INCLUDE_METADATA_DOC);
    }

    public List<String> getFsUris() {
        return this.getList(FS_URIS);
    }

    public String getTopic() {
        return this.getString(TOPIC);
    }

    public Long getPollIntervalMs() { return this.getLong(POLL_INTERVAL_MS); }

    public boolean getIncludeMetadata() { return this.getBoolean(INCLUDE_METADATA); }
}