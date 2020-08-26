package com.github.mmolimar.kafka.connect.fs.policy;

import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import com.github.mmolimar.kafka.connect.fs.file.FileMetadata;
import com.github.mmolimar.kafka.connect.fs.file.Offset;
import com.github.mmolimar.kafka.connect.fs.file.ParsedOffset;
import com.github.mmolimar.kafka.connect.fs.file.reader.FileReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class BulkIncrementalPolicy extends AbstractPolicy {
    public static final String WATCHER_POLICY_PREFIX = FsSourceTaskConfig.POLICY_PREFIX + "watcher.";
    public static final String WATCHER_POLICY_WATCH_FILEPATH = WATCHER_POLICY_PREFIX + "watch.filepath";
    public static final String WATCHER_POLICY_WATCH_PATTERNS = WATCHER_POLICY_PREFIX + "watch.patterns";
    public static final String WATCHER_POLICY_EXCLUSION_PATTERN = WATCHER_POLICY_PREFIX + "exclusion.pattern";
    public static final String WATCHER_POLICY_BATCH_ID_EXTRACTION_PATTERN = WATCHER_POLICY_PREFIX + "batch.id.extraction.pattern";
    public static final String WATCHER_POLICY_BATCH_ID_EXTRACTION_INDEX = WATCHER_POLICY_PREFIX + "batch.id.extraction.index";

    private static final String PATH_OPT = "path";
    private static final String LAST_OPT = "last";
    private static final String LAST_MOD_OPT = "last";
    private static final String WATCH_KEY_OPT = "watchKey";
    private static final String BULK_OPT = "bulk";
    private static final String WATCH_PATTERN_DELINEATOR = ":::";
    private static final String WATCH_PATTERN_PART_DELINEATOR = ":";
    private static final String BATCH_ID_OPT = "batchId";
    private static final String PRIOR_BATCH_ID_OPT = "priorBatchId";
    private static final String WATCHER_METADATA_TYPE = "com.rentpath.filesource.WatcherMetadata";

    private File watchFile;
    private Pattern batchIdExtractionPattern;
    private Pattern exclusionPattern;
    private int batchIdExtractionIndex;
    private List<WatchedPattern> watchedPatterns;
    private long lastRead;

    public BulkIncrementalPolicy(FsSourceTaskConfig conf) throws IOException {
        super(conf);
    }

    @Override
    protected void configPolicy(Map<String, Object> customConfigs) {
        watchFile = new File((String) customConfigs.get(WATCHER_POLICY_WATCH_FILEPATH));
        String batchIdExtractionPatternStr = (String) customConfigs.get(WATCHER_POLICY_BATCH_ID_EXTRACTION_PATTERN);
        if (batchIdExtractionPatternStr == null)
            throw new ConnectException("Batch id extraction pattern not specified!");
        batchIdExtractionPattern = Pattern.compile(batchIdExtractionPatternStr);
        String batchIdExtractionIndexStr = (String) customConfigs.get(WATCHER_POLICY_BATCH_ID_EXTRACTION_INDEX);
        if (batchIdExtractionIndexStr != null)
            batchIdExtractionIndex = Integer.parseInt(batchIdExtractionIndexStr);
        else
            batchIdExtractionIndex = 0;
        String exclusionPatternStr = (String) customConfigs.get(WATCHER_POLICY_EXCLUSION_PATTERN);
        if (exclusionPatternStr != null)
            exclusionPattern = Pattern.compile(exclusionPatternStr);
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
    protected boolean shouldInclude(LocatedFileStatus fileStatus, Pattern pattern, Pattern exclusionPattern) {
        return (super.shouldInclude(fileStatus, pattern, exclusionPattern) &&
                fileStatus.getModificationTime() < lastRead);
    }

    @Override
    public Iterator<FileMetadata> listFiles(FileSystem fs) throws IOException {
        Iterator<FileMetadata> iterator = Collections.emptyIterator();
        for (WatchedPattern pattern : watchedPatterns) {
            iterator = concat(iterator, buildFileIterator(fs, pattern.bulkFilePattern, exclusionPattern, new HashMap<String, Object>() {{
                put(BULK_OPT, true);
                put(WATCH_KEY_OPT, pattern.key);
            }}));
        }
        for (WatchedPattern pattern : watchedPatterns) {
            if (pattern.incrementalFilePattern != null)
                iterator = concat(iterator, buildFileIterator(fs, pattern.incrementalFilePattern, exclusionPattern, new HashMap<String, Object>() {{
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
    public void seekReader(FileMetadata metadata, Map<String, Object> offset, FileReader reader) throws ConnectException, IOException, IllegalArgumentException {
        if (offset != null && offset.get(OFFSET_OPT) != null && metadata.getPath().equals(offset.get(PATH_OPT))) {
            long size = (offset.get(OFFSET_SIZE_OPT) != null) ? ((Long) offset.get(OFFSET_SIZE_OPT)) : 1;
            Offset parsedOffset = new ParsedOffset(((Long) offset.get(OFFSET_OPT)), size);
            reader.seek(parsedOffset);
        }
    }

    @Override
    public Map<String, Object> buildOffsetPartition(FileMetadata metadata) {
        Map<String, Object> value = new HashMap<String, Object>();
        value.put(WATCH_KEY_OPT, metadata.getOpt(WATCH_KEY_OPT));
        return value;
    }

    @Override
    public SchemaAndValue buildKey(FileMetadata metadata, SchemaAndValue snvValue, Map<String, Object> offset) {
        Struct v = (Struct) snvValue.value();
        return new SchemaAndValue(Schema.STRING_SCHEMA, (String) offset.get(BATCH_ID_OPT));
    }

    @Override
    public SchemaAndValue buildMetadata(FileMetadata metadata, long offset, boolean isLast, Map<String, Object> connectorOffset) {
        SchemaBuilder metadataBuilder = SchemaBuilder.struct()
                .name(WATCHER_METADATA_TYPE)
                .optional();
        metadataBuilder.field(PATH_OPT, Schema.STRING_SCHEMA);
        metadataBuilder.field(OFFSET_OPT, Schema.INT64_SCHEMA);
        metadataBuilder.field(LAST_OPT, Schema.BOOLEAN_SCHEMA);
        metadataBuilder.field(BULK_OPT, Schema.BOOLEAN_SCHEMA);
        metadataBuilder.field(WATCH_KEY_OPT, Schema.STRING_SCHEMA);
        metadataBuilder.field(BATCH_ID_OPT, Schema.STRING_SCHEMA);
        metadataBuilder.field(PRIOR_BATCH_ID_OPT, Schema.OPTIONAL_STRING_SCHEMA); // will be null for the very first bulk file we ingest for a given watch key
        Schema schema = metadataBuilder.build();

        Struct metadataValue = new Struct(schema);
        metadataValue.put(PATH_OPT, metadata.getPath());
        metadataValue.put(OFFSET_OPT, offset);
        metadataValue.put(LAST_OPT, isLast);
        metadataValue.put(BULK_OPT, (Boolean) metadata.getOpt(BULK_OPT));
        metadataValue.put(WATCH_KEY_OPT, (String) metadata.getOpt(WATCH_KEY_OPT));
        metadataValue.put(BATCH_ID_OPT, (String) connectorOffset.get(BATCH_ID_OPT));
        metadataValue.put(PRIOR_BATCH_ID_OPT, (String) connectorOffset.get(PRIOR_BATCH_ID_OPT));

        return new SchemaAndValue(schema, metadataValue);
    }

    @Override
    public FileMetadata extractExemplar(List<FileMetadata> batchFileMetadata) {
        List<FileMetadata> bulkFiles = batchFileMetadata.stream()
                .sequential()
                .filter(item -> (Boolean) item.getOpt(BULK_OPT))
                .collect(Collectors.toList());
        if (bulkFiles.size() > 0)
            return bulkFiles.get(0);
        else
            return null;
    }

    private String extractBatchId(FileMetadata exemplarMetadata, FileMetadata sourceMetadata) {
        String firstPath = exemplarMetadata.getPath();
        Matcher m = batchIdExtractionPattern.matcher(firstPath);
        if (m.find())
            return m.group(batchIdExtractionIndex);
        else
            return sourceMetadata.getPath();
    }

    @Override
    public Map<String, Object> buildOffset(FileMetadata metadata, FileMetadata exemplarMetadata, Offset offset, Map<String, Object> priorOffset, boolean isLast) {
        HashMap<String, Object> result = new HashMap<String, Object>();
        result.put(PATH_OPT, metadata.getPath());
        result.put(LAST_MOD_OPT, metadata.getModTime());
        result.put(BULK_OPT, metadata.getOpt(BULK_OPT));
        result.put(OFFSET_OPT, offset.getRecordOffset());
        result.put(OFFSET_SIZE_OPT, offset.getRecordOffsetSize());
        result.put(LAST_IN_CLASS, isLast);
        // The following values aren't needed for resuming reading where we left off.
        // Rather they are for use in appended metadata (for consumption by a downstream processor).
        boolean currentMessageIsBulk = (Boolean) metadata.getOpt(BULK_OPT);
        boolean priorOffsetExists = priorOffset != null;
        boolean priorOffsetWasPartial = priorOffsetExists && !((Boolean) priorOffset.get(BULK_OPT));
        boolean priorOffsetWasLastInClass = (priorOffsetExists
                                             && priorOffset.get(LAST_IN_CLASS) != null
                                             && ((Boolean)priorOffset.get(LAST_IN_CLASS)));
        boolean shouldRotateBatchId = (currentMessageIsBulk
                                       && (!priorOffsetExists
                                           || priorOffsetWasPartial
                                           || priorOffsetWasLastInClass));
        if (shouldRotateBatchId) {
            result.put(BATCH_ID_OPT, extractBatchId(exemplarMetadata, metadata));
            result.put(PRIOR_BATCH_ID_OPT, priorOffsetExists ? ((String) priorOffset.get(BATCH_ID_OPT)) : null);
        } else {
            result.put(BATCH_ID_OPT, (String) priorOffset.get(BATCH_ID_OPT));
            result.put(PRIOR_BATCH_ID_OPT, (String) priorOffset.get(PRIOR_BATCH_ID_OPT));
        }
        return result;
    }

    @Override
    protected boolean shouldOffer(FileMetadata metadata, Map<String, Object> offset) {
        // Note that when the connector is first run, the offset will be null.
        if (offset == null) return true;

        if (metadata.getPath().equals((String) offset.get(PATH_OPT))) {
            return true;
        }
        return (metadata.getModTime() > (long) offset.get(LAST_MOD_OPT)) ||
                ((metadata.getModTime() == (long) offset.get(LAST_MOD_OPT)) &&
                        ((metadata.getPath().compareTo((String) offset.get(PATH_OPT))) > 0));
    }

    static class WatchedPattern {
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
