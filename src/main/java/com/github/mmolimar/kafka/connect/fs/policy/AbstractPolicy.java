package com.github.mmolimar.kafka.connect.fs.policy;

import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import com.github.mmolimar.kafka.connect.fs.file.FileMetadata;
import com.github.mmolimar.kafka.connect.fs.file.Offset;
import com.github.mmolimar.kafka.connect.fs.file.ParsedOffset;
import com.github.mmolimar.kafka.connect.fs.file.reader.FileReader;
import com.github.mmolimar.kafka.connect.fs.util.ReflectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public abstract class AbstractPolicy implements Policy {

    private final Logger log = LoggerFactory.getLogger(getClass());

    protected final List<FileSystem> fileSystems;
    protected final Pattern filePattern;

    private final FsSourceTaskConfig conf;
    private final AtomicInteger executions;
    protected final boolean recursive;
    private boolean interrupted;
    public static final String FINAL_OPT = "final";
    public static final String OFFSET_OPT = "offset";
    public static final String LAST_IN_CLASS = "lastInClass";
    public static final String OFFSET_SIZE_OPT = "offsetSize";

    public AbstractPolicy(FsSourceTaskConfig conf) throws IOException {
        this.fileSystems = new ArrayList<>();
        this.conf = conf;
        this.executions = new AtomicInteger(0);
        this.recursive = conf.getBoolean(FsSourceTaskConfig.POLICY_RECURSIVE);
        this.filePattern = Pattern.compile(conf.getString(FsSourceTaskConfig.POLICY_REGEXP));
        this.interrupted = false;

        Map<String, Object> customConfigs = customConfigs();
        logAll(customConfigs);
        configFs(customConfigs);
        configPolicy(customConfigs);
    }

    private Map<String, Object> customConfigs() {
        return conf.originals().entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(FsSourceTaskConfig.POLICY_PREFIX))
                .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));
    }

    private void configFs(Map<String, Object> customConfigs) throws IOException {
        for (String uri : this.conf.getFsUris()) {
            Configuration fsConfig = new Configuration();
            customConfigs.entrySet().stream()
                    .filter(entry -> entry.getKey().startsWith(FsSourceTaskConfig.POLICY_PREFIX_FS))
                    .forEach(entry -> fsConfig.set(entry.getKey().replace(FsSourceTaskConfig.POLICY_PREFIX_FS, ""),
                            (String) entry.getValue()));

            Path workingDir = new Path(convert(uri));
            FileSystem fs = FileSystem.newInstance(workingDir.toUri(), fsConfig);
            fs.setWorkingDirectory(workingDir);
            this.fileSystems.add(fs);
        }
    }

    private String convert(String uri) {
        String converted = uri;
        LocalDateTime dateTime = LocalDateTime.now();
        DateTimeFormatter formatter;

        Pattern pattern = Pattern.compile("\\$\\{([a-zA-Z]+)}");
        Matcher matcher = pattern.matcher(uri);
        while (matcher.find()) {
            try {
                formatter = DateTimeFormatter.ofPattern(matcher.group(1));
                converted = converted.replaceAll("\\$\\{" + matcher.group(1) + "}", dateTime.format(formatter));
            } catch (Exception e) {
                throw new IllegalArgumentException("Cannot convert dynamic URI: " + matcher.group(1), e);
            }
        }

        return converted;
    }

    protected abstract void configPolicy(Map<String, Object> customConfigs);

    @Override
    public List<String> getURIs() {
        List<String> uris = new ArrayList<>();
        fileSystems.forEach(fs -> uris.add(fs.getWorkingDirectory().toString()));
        return uris;
    }

    @Override
    public Iterator<FileMetadata> execute() throws IOException {
        if (hasEnded()) {
            throw new IllegalWorkerStateException("Policy has ended. Cannot be retried");
        }
        preCheck();

        Iterator<FileMetadata> files = Collections.emptyIterator();
        for (FileSystem fs : fileSystems) {
            files = concat(files, listFiles(fs));
        }
        executions.incrementAndGet();

        postCheck();

        return files;
    }

    @Override
    public void interrupt() {
        interrupted = true;
    }

    protected void preCheck() {
    }

    protected void postCheck() {
    }

    protected boolean shouldInclude(LocatedFileStatus fileStatus, Pattern pattern, Pattern exclusionPattern) {
        String path = fileStatus.getPath().toString();
        return (
                fileStatus.isFile()
                && pattern.matcher(path).find()
                && (exclusionPattern == null || !(exclusionPattern.matcher(path).find()))
        );
    }

    protected Iterator<FileMetadata> buildFileIterator(FileSystem fs, Pattern pattern, Pattern exclusionPattern, Map<String, Object> opts) throws IOException {
        return new Iterator<FileMetadata>() {
            RemoteIterator<LocatedFileStatus> it = fs.listFiles(fs.getWorkingDirectory(), recursive);
            LocatedFileStatus current = null;
            boolean previous = false;

            @Override
            public boolean hasNext() {
                try {
                    if (current == null) {
                        if (!it.hasNext()) return false;
                        current = it.next();
                        return hasNext();
                    }
                    if (shouldInclude(current, pattern, exclusionPattern)) {
                        return true;
                    }
                    current = null;
                    return hasNext();
                } catch (IOException ioe) {
                    throw new ConnectException(ioe);
                }
            }

            @Override
            public FileMetadata next() {
                if (!hasNext() && current == null) {
                    throw new NoSuchElementException("There are no more items");
                }
                LocatedFileStatus nextFile = current;
                current = null;
                FileMetadata metadata = toMetadata(nextFile, hasNext(), opts);
                return metadata;
            }
        };
    }

    protected Iterator<FileMetadata> buildFileIterator(FileSystem fs, Pattern pattern) throws IOException {
        return buildFileIterator(fs, pattern, null, null);
    }

    public Iterator<FileMetadata> listFiles(FileSystem fs) throws IOException {
        return buildFileIterator(fs, filePattern);
    }

    @Override
    public final boolean hasEnded() {
        return interrupted || isPolicyCompleted();
    }

    protected abstract boolean isPolicyCompleted();

    public final int getExecutions() {
        return executions.get();
    }

    protected FileMetadata toMetadata(LocatedFileStatus fileStatus, boolean continues, Map<String, Object> opts) {
        List<FileMetadata.BlockInfo> blocks = new ArrayList<>();

        blocks.addAll(Arrays.stream(fileStatus.getBlockLocations())
                .map(block ->
                        new FileMetadata.BlockInfo(block.getOffset(), block.getLength(), block.isCorrupt()))
                .collect(Collectors.toList()));

        FileMetadata metadata = new FileMetadata(fileStatus.getPath().toString(), fileStatus.getLen(), fileStatus.getModificationTime(), blocks);
        if (continues)
            metadata.setOpt(FINAL_OPT, false);
        else
            metadata.setOpt(FINAL_OPT, true);
        if (opts != null)
            for (String key : opts.keySet())
                metadata.setOpt(key, opts.get(key));

        return metadata;
    }

    @Override
    public void seekReader(FileMetadata metadata, Map<String, Object> offset, FileReader reader) throws ConnectException, IOException, IllegalArgumentException {
        if (offset != null && offset.get(OFFSET_OPT) != null) {
            long size = (offset.get(OFFSET_SIZE_OPT) != null) ? ((Long)offset.get(OFFSET_SIZE_OPT)) : 1;
            reader.seek(new ParsedOffset(((Long)offset.get(OFFSET_OPT)), size));
        }
    }

    protected boolean shouldOffer(FileMetadata metadata, Map<String, Object> offset) {
        return true;
    }

    @Override
    public FileReader offer(FileMetadata metadata, Map<String, Object> lastOffset) throws IOException {
        if (!shouldOffer(metadata, lastOffset))
            return null;

        FileSystem current = fileSystems.stream()
                .filter(fs -> metadata.getPath().startsWith(fs.getWorkingDirectory().toString()))
                .findFirst().orElse(null);

        FileReader reader;
        try {
            reader = ReflectionUtils.makeReader((Class<? extends FileReader>) conf.getClass(FsSourceTaskConfig.FILE_READER_CLASS),
                    current, new Path(metadata.getPath()), conf.originals());
        } catch (Throwable t) {
            throw new ConnectException("An error has occurred when creating reader for file: " + metadata.getPath(), t);
        }
        return reader;
    }

    Iterator<FileMetadata> concat(final Iterator<FileMetadata> it1,
                                  final Iterator<FileMetadata> it2) {
        return new Iterator<FileMetadata>() {

            @Override
            public boolean hasNext() {
                return it1.hasNext() || it2.hasNext();
            }

            @Override
            public FileMetadata next() {
                return it1.hasNext() ? it1.next() : it2.next();
            }
        };
    }

    @Override
    public void close() throws IOException {
        for (FileSystem fs : fileSystems) {
            fs.close();
        }
    }

    private void logAll(Map<String, Object> conf) {
        StringBuilder b = new StringBuilder();
        b.append(getClass().getSimpleName());
        b.append(" values: ");
        b.append(Utils.NL);
        for (Map.Entry<String, Object> entry : conf.entrySet()) {
            b.append('\t');
            b.append(entry.getKey());
            b.append(" = ");
            b.append(entry.getValue());
            b.append(Utils.NL);
        }
        log.info(b.toString());
    }

    @Override
    public Map<String, Object> buildOffsetPartition(FileMetadata metadata) {
        Map<String, Object> value = new HashMap<String, Object>() {
            {
                put("path", metadata.getPath());
            }
        };
        return value;
    }

    @Override
    public SchemaAndValue buildKey(FileMetadata metadata, SchemaAndValue snvValue, Map<String, Object> offset) {
        SchemaBuilder builder = SchemaBuilder.struct();
        builder.field("path", SchemaBuilder.STRING_SCHEMA);
        Schema schema = builder.build();

        Struct value = new Struct(schema);
        value.put("path", metadata.getPath());
        return new SchemaAndValue(schema, value);
    }

    @Override
    public SchemaAndValue buildMetadata(FileMetadata metadata, long offset, boolean isLast, Map<String, Object> connectorOffset) {
        SchemaBuilder metadataBuilder = SchemaBuilder.struct()
                .name("com.rentpath.filesource.Metadata")
                .optional();
        metadataBuilder.field("path", Schema.STRING_SCHEMA);
        Schema schema = metadataBuilder.build();

        Struct metadataValue = new Struct(schema);
        metadataValue.put("path", metadata.getPath());

        return new SchemaAndValue(schema, metadataValue);
    }

    @Override
    public Map<String, Object> buildOffset(FileMetadata metadata, FileMetadata exemplarMetadata, Offset offset, Map<String, Object> priorOffset, boolean isLast) {
        HashMap<String, Object> result = new HashMap<String, Object>();
        result.put(OFFSET_OPT, offset.getRecordOffset());
        result.put(OFFSET_SIZE_OPT, offset.getRecordOffsetSize());
        return result;
    }

    @Override
    public FileMetadata extractExemplar(List<FileMetadata> batchFileMetadata) {
        if (batchFileMetadata.size() > 0)
            return batchFileMetadata.get(0);
        else
            return null;
    }
}
