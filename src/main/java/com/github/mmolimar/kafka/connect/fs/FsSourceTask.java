package com.github.mmolimar.kafka.connect.fs;

import com.github.mmolimar.kafka.connect.fs.file.FileMetadata;
import com.github.mmolimar.kafka.connect.fs.file.Offset;
import com.github.mmolimar.kafka.connect.fs.file.reader.FileReader;
import com.github.mmolimar.kafka.connect.fs.policy.Policy;
import com.github.mmolimar.kafka.connect.fs.util.ReflectionUtils;
import com.github.mmolimar.kafka.connect.fs.util.Version;
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

    private AtomicBoolean stop;
    private FsSourceTaskConfig config;
    private Policy policy;
    private long maxBatchSize;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
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

    private SchemaAndValue appendMetadata(SchemaAndValue source, FileMetadata metadata, boolean isLast) {
        Schema sourceSchema = source.schema();
        Struct sourceValue = (Struct) source.value();
        SchemaBuilder builder = SchemaBuilder.struct()
                .name(sourceSchema.name())
                .optional();
        for (Field field : sourceSchema.fields()) {
            builder.field(field.name(), field.schema());
        }

        SchemaAndValue policyMetadata = policy.buildMetadata(metadata, isLast);
        builder.field("_file_metadata", policyMetadata.schema());
        Schema schema = builder.build();

        Struct value = new Struct(schema);
        for (Field field : sourceSchema.fields()) {
            value.put(field.name(), sourceValue.get(field.name()));
        }

        value.put("_file_metadata", policyMetadata.value());
        return new SchemaAndValue(schema, value);
    }

    // Compare by mod time (falling back to path comparison for equal mod times).
    private int compareFileMetadata(FileMetadata f1, FileMetadata f2) {
      if (f1.getModTime() == f2.getModTime())
          return f1.getPath().compareTo(f2.getPath());
      return (new Long(f1.getModTime())).compareTo(new Long(f2.getModTime()));
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        while (stop != null && !stop.get() && !policy.hasEnded()) {
            if (config.getPollIntervalMs() > 0)
                Thread.sleep(config.getPollIntervalMs());
            log.trace("Polling for new data");

            final List<SourceRecord> results = new ArrayList<>();
            List<FileMetadata> files = filesToProcess();
            // Sort files by last mod time so that we handle older files first.
            Collections.sort(files, (FileMetadata f1, FileMetadata f2) -> compareFileMetadata(f1, f2));

            int count = 0;
            for (FileMetadata metadata : files) {
                Map<String, Object> lastOffset = context.offsetStorageReader().offset(policy.buildPartition(metadata));
                try (FileReader reader = policy.offer(metadata, lastOffset)) {
                    if (reader != null) {
                        while (reader.hasNext() && (maxBatchSize == 0 || count < maxBatchSize)) {
                            log.info("Processing records for file {}", metadata);
                            SchemaAndValue sAndV = reader.next();
                            log.info("reader.next {}", sAndV); // FIXME del
                            boolean isLast = !reader.hasNext();
                            if (config.getIncludeMetadata())
                                sAndV = appendMetadata(sAndV, metadata, isLast);
                            log.info("post-appendMetadata {}", sAndV); // FIXME del
                            results.add(convert(metadata, policy, lastOffset, reader.currentOffset(), sAndV, isLast));
                            count++;
                        }
                    }
                } catch (ConnectException | IOException e) {
                    //when an exception happens reading a file, the connector continues
                    log.error("Error reading file from FS: " + metadata.getPath() + ". Keep going...", e);
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
        } catch (IOException | ConnectException e) {
            //when an exception happens executing the policy, the connector continues
            log.error("Cannot retrieve files to process from FS: " + policy.getURIs() + ". Keep going...", e);
            return Collections.EMPTY_LIST;
        }
    }

    private <T> Stream<T> asStream(Iterator<T> src) {
        Iterable<T> iterable = () -> src;
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    private SourceRecord convert(FileMetadata metadata, Policy policy, Map<String, Object> lastOffset, Offset recordOffset, SchemaAndValue snvValue, boolean isLast) {
        SchemaAndValue snvKey = policy.buildKey(metadata);
        log.info("convert snvKey.schema={} snvKey.value={} snvValue.schema={} snvValue.value={}", snvKey.schema(), snvKey.value(), snvValue.schema(), snvValue.value()); // FIXME del
        return new SourceRecord(
                policy.buildPartition(metadata),
                policy.buildOffset(metadata, lastOffset, recordOffset, isLast),
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
