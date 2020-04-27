package com.github.mmolimar.kafka.connect.fs.policy;

import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import com.github.mmolimar.kafka.connect.fs.file.FileMetadata;
import com.github.mmolimar.kafka.connect.fs.file.Offset;
import com.github.mmolimar.kafka.connect.fs.file.reader.FileReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

public class BulkIncrementalPolicy extends AbstractPolicy {
    public static final String WATCHER_POLICY_PREFIX = FsSourceTaskConfig.POLICY_PREFIX + "watcher.";
    public static final String WATCHER_POLICY_WATCH_FILEPATH = WATCHER_POLICY_PREFIX + "watch.filepath";
    public static final String WATCHER_POLICY_WATCH_PATTERNS = WATCHER_POLICY_PREFIX + "watch.patterns";

    private static final String WATCH_KEY_OPT = "watchKey";
    private static final String BULK_OPT = "bulk";
    private static final String WATCH_PATTERN_DELINEATOR = ":::";
    private static final String WATCH_PATTERN_PART_DELINEATOR = ":";

    private File watchFile;
    private Pattern offsetKeySearchPattern;
    private String offsetKeyReplacement;
    private List<WatchedPattern> watchedPatterns;
    private long lastRead;

    private final Logger log = LoggerFactory.getLogger(getClass());

    public BulkIncrementalPolicy(FsSourceTaskConfig conf) throws IOException {
        super(conf);
    }

    @Override
    protected void configPolicy(Map<String, Object> customConfigs) {
        watchFile = new File((String) customConfigs.get(WATCHER_POLICY_WATCH_FILEPATH));
        watchedPatterns = new ArrayList<>();
        String watchPatternStr = (String) customConfigs.get(WATCHER_POLICY_WATCH_PATTERNS);
        if (watchPatternStr == null || watchPatternStr.equals(""))
            throw new ConnectException("Watch patterns not specified!");
        String[] watchPatternEntries = watchPatternStr.split(WATCH_PATTERN_DELINEATOR);
        for (String watchPatternEntry : watchPatternEntries) {
            String[] watchPatternParts = watchPatternEntry.split(WATCH_PATTERN_PART_DELINEATOR);
            WatchedPattern watchedPattern;
            if (watchPatternParts.length == 3)
                watchedPattern = new WatchedPattern(watchPatternParts[0], Pattern.compile(watchPatternParts[1]), Pattern.compile(watchPatternParts[2]));
            else if (watchPatternParts.length == 2)
                watchedPattern = new WatchedPattern(watchPatternParts[0], Pattern.compile(watchPatternParts[1]), null);
            else
                throw new ConnectException("Incorrectly specified watch pattern! Should be watchKey:bulkPattern:incrementalPattern");
            watchedPatterns.add(watchedPattern);
        }
    }

    @Override
    protected void preCheck() {
        lastRead = watchFile.lastModified();
    }

    @Override
    protected boolean shouldInclude(LocatedFileStatus fileStatus, Pattern pattern) {
        return (super.shouldInclude(fileStatus, pattern) &&
                fileStatus.getModificationTime() < lastRead);
    }

    @Override
    public Iterator<FileMetadata> listFiles(FileSystem fs) throws IOException {
        Iterator<FileMetadata> iterator = Collections.emptyIterator();
        for (WatchedPattern pattern : watchedPatterns) {
            iterator = concat(iterator, buildFileIterator(fs, pattern.bulkFilePattern, new HashMap<String, Object>() {{
                put(BULK_OPT, true);
                put(WATCH_KEY_OPT, pattern.key);
            }}));
        }
        for (WatchedPattern pattern : watchedPatterns) {
            if (pattern.incrementalFilePattern != null)
                iterator = concat(iterator, buildFileIterator(fs, pattern.incrementalFilePattern, new HashMap<String, Object>() {{
                    put(BULK_OPT, false);
                    put(WATCH_KEY_OPT, pattern.key);
                }}));
        }
        return iterator;
    }

    @Override
    protected boolean isPolicyCompleted() {
        return false;
    }

    @Override
    public FileReader seekReader(FileMetadata metadata, Map<String, Object> offset, FileReader reader) {
        if (offset != null && offset.get("timestamp") != null && offset.get("path") != null) {
            Long timestamp = ((Long) offset.get("timestamp"));
            if (timestamp == lastRead && offset.get("offset") != null) {
                if (metadata.getPath().equals(offset.get("path")))
                    reader.seek(() -> (Long) offset.get("offset"));
                else
                    return null;
            }
        }
        return reader;
    }

    private String derivePath(FileMetadata metadata) {
        if (offsetKeySearchPattern == null)
            return metadata.getPath();
        return offsetKeySearchPattern.matcher(metadata.getPath()).replaceAll(offsetKeyReplacement);
    }

    @Override
    public Map<String, Object> buildPartition(FileMetadata metadata) {
        Map<String, Object> value = new HashMap<String, Object>();
        value.put(WATCH_KEY_OPT, metadata.getOpt(WATCH_KEY_OPT));
        return value;
    }

    @Override
    public SchemaAndValue buildKey(FileMetadata metadata) {
        // build schema
        SchemaBuilder builder = SchemaBuilder.struct()
                .name("com.rentpath.filesource.WatchPolicyKey")
                .optional();
        builder.field(WATCH_KEY_OPT, Schema.STRING_SCHEMA);
        Schema schema = builder.build();

        // build value
        Struct value = new Struct(schema);
        value.put(WATCH_KEY_OPT, metadata.getOpt(WATCH_KEY_OPT));

        return new SchemaAndValue(schema, value);
    }

    // FIXME: Is this really the metadata we want?
    @Override
    public SchemaAndValue buildMetadata(FileMetadata metadata, boolean isLast) {
        SchemaBuilder metadataBuilder = SchemaBuilder.struct()
                .name("com.rentpath.filesource.WatcherMetadata")
                .optional();
        metadataBuilder.field("path", Schema.STRING_SCHEMA);
        metadataBuilder.field("last", Schema.BOOLEAN_SCHEMA);
        metadataBuilder.field("bulk", Schema.BOOLEAN_SCHEMA);
        Schema schema = metadataBuilder.build();

        Struct metadataValue = new Struct(schema);
        metadataValue.put("path", metadata.getPath());
        metadataValue.put("last", isLast);
        metadataValue.put("bulk", (Boolean) metadata.getOpt(BULK_OPT));

        return new SchemaAndValue(schema, metadataValue);
    }

    // FIXME: do we really need to pass lastOffset and isLast ???
    @Override
    public Map<String, Object> buildOffset(FileMetadata metadata, Map<String, Object> lastOffset, Offset recordOffset, boolean isLast) {
        return new HashMap<String, Object>() {
            {
                put("path", metadata.getPath());
                put("lastMod", metadata.getModTime());
                put("offset", recordOffset.getRecordOffset());
            }
        };
    }

    @Override
    protected boolean shouldOffer(FileMetadata metadata, Map<String, Object> offset) {
        log.info("shouldOffer metadata={} offset={}", metadata, offset); // FIXME del
        if (offset == null) return true;
        if (metadata.getPath().equals((String) offset.get("path"))) {
          // FIXME: make sure this is correct; consider off-by-one and the possibility of having read a file before the write finished (partial record)
          // Actually this is definitely NOT correct!
          // The offset value isn't bytes but records (or perhaps lines). Maybe need some way to track isLast/finished for a file after all...
          return metadata.getLen() > (long) offset.get("offset");
        }
        return (metadata.getModTime() > (long) offset.get("lastMod")) ||
               ((metadata.getModTime() == (long) offset.get("lastMod")) &&
                ((metadata.getPath().compareTo((String) offset.get("path"))) > 0)); }

    class WatchedPattern {
        String key;
        Pattern bulkFilePattern;
        Pattern incrementalFilePattern;

        WatchedPattern(String key, Pattern bulkFilePattern, Pattern incrementalFilePattern) {
            this.key = key;
            this.bulkFilePattern = bulkFilePattern;
            this.incrementalFilePattern = incrementalFilePattern;
        }
    }
}
