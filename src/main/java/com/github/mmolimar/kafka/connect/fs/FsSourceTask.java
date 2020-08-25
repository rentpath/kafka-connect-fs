package com.github.mmolimar.kafka.connect.fs;

import com.github.mmolimar.kafka.connect.fs.file.FileMetadata;
import com.github.mmolimar.kafka.connect.fs.file.Offset;
import com.github.mmolimar.kafka.connect.fs.file.reader.FileReader;
import com.github.mmolimar.kafka.connect.fs.policy.AbstractPolicy;
import com.github.mmolimar.kafka.connect.fs.policy.Policy;
import com.github.mmolimar.kafka.connect.fs.util.ReflectionUtils;
import com.github.mmolimar.kafka.connect.fs.util.Version;
import org.apache.hadoop.fs.FSError;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class FsSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(FsSourceTask.class);
    private static final String FILE_METADATA_KEY = "_file_metadata";

    private AtomicBoolean stop;
    private FsSourceTaskConfig config;
    private Policy policy;
    private long maxBatchSize;
    private boolean firstPoll;
    private Map<Object, Object> offsets;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        firstPoll = true;
        offsets = new HashMap<Object, Object>();
        try {
            config = new FsSourceTaskConfig(properties);
            maxBatchSize = config.getLong(FsSourceTaskConfig.MAX_BATCH_SIZE);

            if (config.getClass(FsSourceTaskConfig.POLICY_CLASS).isAssignableFrom(Policy.class)) {
                throw new ConfigException("Policy class " +
                        config.getClass(FsSourceTaskConfig.POLICY_CLASS) + "is not a sublass of " + Policy.class);
            }
            if (config.getClass(FsSourceTaskConfig.FILE_READER_CLASS).isAssignableFrom(FileReader.class)) {
                throw new ConfigException("FileReader class " +
                        config.getClass(FsSourceTaskConfig.FILE_READER_CLASS) + "is not a sublass of " + FileReader.class);
            }

            Class<Policy> policyClass = (Class<Policy>) Class.forName(properties.get(FsSourceTaskConfig.POLICY_CLASS));
            FsSourceTaskConfig taskConfig = new FsSourceTaskConfig(properties);
            policy = ReflectionUtils.makePolicy(policyClass, taskConfig);
        } catch (ConfigException ce) {
            log.error("Couldn't start FsSourceTask:", ce);
            throw new ConnectException("Couldn't start FsSourceTask due to configuration error", ce);
        } catch (Throwable t) {
            log.error("Couldn't start FsSourceConnector:", t);
            throw new ConnectException("A problem has occurred reading configuration:" + t.getMessage());
        }

        stop = new AtomicBoolean(false);
    }

    private SchemaAndValue appendMetadata(SchemaAndValue source, FileMetadata metadata, long offset, boolean isLast, Map<String, Object> connectorOffset) {
        Schema sourceSchema = source.schema();
        Struct sourceValue = (Struct) source.value();
        SchemaBuilder builder = SchemaBuilder.struct()
                .name(sourceSchema.name())
                .optional();
        for (Field field : sourceSchema.fields()) {
            builder.field(field.name(), field.schema());
        }

        SchemaAndValue policyMetadata = policy.buildMetadata(metadata, offset, isLast, connectorOffset);
        builder.field(FILE_METADATA_KEY, policyMetadata.schema());
        Schema schema = builder.build();

        Struct value = new Struct(schema);
        for (Field field : sourceSchema.fields()) {
            value.put(field.name(), sourceValue.get(field.name()));
        }

        value.put(FILE_METADATA_KEY, policyMetadata.value());
        return new SchemaAndValue(schema, value);
    }

    // Compare by mod time (falling back to path comparison for equal mod times).
    private int compareFileMetadata(FileMetadata f1, FileMetadata f2) {
      if (f1.getModTime() == f2.getModTime())
          return f1.getPath().compareTo(f2.getPath());
      return (new Long(f1.getModTime())).compareTo(new Long(f2.getModTime()));
    }

    private Map<String, Object> getOffset(Map<String, Object> partition) {
        Map<String, Object> offset = (Map<String, Object>) offsets.get(partition);
        if(offset == null) {
            offset = context.offsetStorageReader().offset(partition);
            offsets.put(partition, offset);
        }
        return offset;
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        while (stop != null && !stop.get() && !policy.hasEnded()) {
            if ((config.getPollIntervalMs() > 0) && !firstPoll) {
                Thread.sleep(config.getPollIntervalMs());
            }
            firstPoll = false;
            log.trace("Polling for new data");

            final List<SourceRecord> results = new ArrayList<>();
            List<FileMetadata> files = filesToProcess();
            // Sort files by last mod time so that we handle older files first.
            Collections.sort(files, (FileMetadata f1, FileMetadata f2) -> compareFileMetadata(f1, f2));
            FileMetadata exemplarMetadata = policy.extractExemplar(files);
            if (exemplarMetadata == null)
                return new ArrayList<SourceRecord>();
            int count = 0;
            for (FileMetadata metadata : files) {
                Map<String, Object> partition = policy.buildOffsetPartition(metadata);
                Map<String, Object> lastOffset = getOffset(partition);
                try (FileReader reader = policy.offer(metadata, lastOffset)) {
                    if (reader != null) {
                        log.info("Processing records for file {}", metadata);
                        policy.seekReader(metadata, lastOffset, reader);
                        while (reader.hasNext() && (maxBatchSize == 0 || count < maxBatchSize)) {
                            Offset currentOffset = reader.currentOffset();
                            SchemaAndValue sAndV = reader.next();
                            // only mark last if both the final file in the iterator *and* the last record in the file
                            boolean isLast = ((Boolean) metadata.getOpt(AbstractPolicy.FINAL_OPT)) && !reader.hasNext();
                            Map<String, Object> offset = policy.buildOffset(metadata, exemplarMetadata, currentOffset, lastOffset, isLast);
                            if (config.getIncludeMetadata()) {
                                sAndV = appendMetadata(sAndV, metadata, currentOffset.getRecordOffset(), isLast, offset);
                            }
                            results.add(convert(metadata, policy, partition, offset, sAndV));
                            offsets.put(partition, offset);
                            lastOffset = offset;
                            count++;
                        }
                    }
                } catch (ConnectException e) {
                    //when an exception happens reading a file, the connector continues
                    log.error("Possible error reading file from FS: " + metadata.getPath() + ". Keep going...", e);
                } catch (IOException e) {
                    log.error("Error reading file from FS: " + metadata.getPath() + ". Keep going...", e);
                } catch (FSError e) {
                    log.error("Filesystem Error reading file from FS: " + metadata.getPath() + ". Keep going...", e);
                }
            }
            return results;
        }
        return null;
    }

    private List<FileMetadata> filesToProcess() {
        try {
            return asStream(policy.execute())
                    .filter(metadata -> metadata.getLen() > 0)
                    .collect(Collectors.toList());
        } catch (IOException | RuntimeException e) {
            //when an exception happens executing the policy, the connector continues
            log.error("Cannot retrieve files to process from FS: " + policy.getURIs() + ". Keep going...", e);
            return Collections.EMPTY_LIST;
        }
    }

    private <T> Stream<T> asStream(Iterator<T> src) {
        Iterable<T> iterable = () -> src;
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    private SourceRecord convert(FileMetadata metadata, Policy policy, Map<String, Object> partition, Map<String, Object> offset, SchemaAndValue snvValue) {
        SchemaAndValue snvKey = policy.buildKey(metadata, snvValue, offset);
        return new SourceRecord(
            partition,
            offset,
            config.getTopic(),
            snvKey.schema(),
            snvKey.value(),
            snvValue.schema(),
            snvValue.value()
        );
    }

    @Override
    public void stop() {
        if (stop != null) {
            stop.set(true);
        }
        if (policy != null) {
            policy.interrupt();
        }
    }
}
