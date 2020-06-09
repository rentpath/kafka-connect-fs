package com.github.mmolimar.kafka.connect.fs;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class FsSourceTaskConfig extends FsSourceConnectorConfig {
    public static final String MAX_BATCH_SIZE = "max.batch.size";
    public static final String MAX_BATCH_SIZE_DOC = "Maximum number of records to extract from the file per polling pass. Defaults to unlimited.";
    public static final long MAX_BATCH_SIZE_DEFAULT = 0;

    public static final String POLICY_PREFIX = "policy.";
    public static final String FILE_READER_PREFIX = "file_reader.";

    public static final String POLICY_CLASS = POLICY_PREFIX + "class";
    private static final String POLICY_CLASS_DOC = "Policy class to apply to this task.";

    public static final String POLICY_RECURSIVE = POLICY_PREFIX + "recursive";
    private static final String POLICY_RECURSIVE_DOC = "Flag to activate traversed recursion in subdirectories when listing files.";

    public static final String POLICY_REGEXP = POLICY_PREFIX + "regexp";
    private static final String POLICY_REGEXP_DOC = "Regular expression to filter files from the FS.";

    public static final String POLICY_PREFIX_FS = POLICY_PREFIX + "fs.";

    public static final String FILE_READER_CLASS = FILE_READER_PREFIX + "class";
    private static final String FILE_READER_CLASS_DOC = "File reader class to read files from the FS.";

    public static final String POLICY_PROPAGATE_ERRORS = POLICY_PREFIX + "propagate.errors";
    private static final String POLICY_PROPAGATE_ERRORS_DOC = "Whether or not the policy should propagate or continue when encountering an error";

    public FsSourceTaskConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public FsSourceTaskConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        return FsSourceConnectorConfig.conf()
                .define(MAX_BATCH_SIZE, ConfigDef.Type.LONG, MAX_BATCH_SIZE_DEFAULT, ConfigDef.Importance.HIGH, MAX_BATCH_SIZE_DOC)
                .define(POLICY_CLASS, ConfigDef.Type.CLASS, ConfigDef.Importance.HIGH, POLICY_CLASS_DOC)
                .define(POLICY_RECURSIVE, ConfigDef.Type.BOOLEAN, Boolean.TRUE, ConfigDef.Importance.LOW, POLICY_RECURSIVE_DOC)
                .define(POLICY_REGEXP, ConfigDef.Type.STRING, ".*", ConfigDef.Importance.MEDIUM, POLICY_REGEXP_DOC)
                .define(POLICY_PROPAGATE_ERRORS, ConfigDef.Type.BOOLEAN, Boolean.FALSE, ConfigDef.Importance.MEDIUM, POLICY_PROPAGATE_ERRORS_DOC)
                .define(FILE_READER_CLASS, ConfigDef.Type.CLASS, ConfigDef.Importance.HIGH, FILE_READER_CLASS_DOC);
    }

}
