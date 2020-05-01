package com.github.mmolimar.kafka.connect.fs.policy.local;

import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import com.github.mmolimar.kafka.connect.fs.file.reader.TextFileReader;
import com.github.mmolimar.kafka.connect.fs.policy.BulkIncrementalPolicy;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class BulkIncrementalPolicyTest extends LocalPolicyTestBase {
    @Override
    public void initPolicy() throws Throwable {
        File watchFile = new File(localDir.toFile(), "watchfile");
        watchFile.createNewFile();
        watchFile.setLastModified(Long.MAX_VALUE);
        super.initPolicy();
    }

    @BeforeClass
    public static void setUp() throws IOException {
        directories = new ArrayList<Path>() {{
            add(new Path(fsUri.toString(), UUID.randomUUID().toString()));
            add(new Path(fsUri.toString(), UUID.randomUUID().toString()));
        }};
        for (Path dir : directories) {
            fs.mkdirs(dir);
        }
        File watchFile = new File(localDir.toFile(), "watchfile");

        Map<String, String> cfg = new HashMap<String, String>() {{
            String uris[] = directories.stream().map(dir -> dir.toString())
                    .toArray(size -> new String[size]);
            put(FsSourceTaskConfig.FS_URIS, String.join(",", uris));
            put(FsSourceTaskConfig.TOPIC, "topic_test");
            put(FsSourceTaskConfig.POLICY_CLASS, BulkIncrementalPolicy.class.getName());
            put(FsSourceTaskConfig.FILE_READER_CLASS, TextFileReader.class.getName());
            put(FsSourceTaskConfig.POLICY_PREFIX_FS + "dfs.data.dir", "test");
            put(FsSourceTaskConfig.POLICY_PREFIX_FS + "fs.default.name", "test");
            put(BulkIncrementalPolicy.WATCHER_POLICY_WATCH_FILEPATH, watchFile.getAbsolutePath());
            put(BulkIncrementalPolicy.WATCHER_POLICY_WATCH_PATTERNS, "someKey:\\/[0-9]*\\.txt$");
        }};

        taskConfig = new FsSourceTaskConfig(cfg);
    }

    @Override
    public void hasEnded() throws IOException {
    }

    @Override
    public void execPolicyAlreadyEnded() throws IOException {
        throw new IllegalWorkerStateException("Dummy Exception");
    }

    @Override
    public void cleanDirs() throws IOException {
        File watchFile = new File(localDir.toFile(), "watchfile");
        if (watchFile.exists())
            watchFile.delete();
        super.cleanDirs();
    }
}
