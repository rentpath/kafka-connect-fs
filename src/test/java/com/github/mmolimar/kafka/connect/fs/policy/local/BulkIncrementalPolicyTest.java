package com.github.mmolimar.kafka.connect.fs.policy.local;

import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import com.github.mmolimar.kafka.connect.fs.file.FileMetadata;
import com.github.mmolimar.kafka.connect.fs.file.reader.TextFileReader;
import com.github.mmolimar.kafka.connect.fs.policy.BulkIncrementalPolicy;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

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
            put(BulkIncrementalPolicy.WATCHER_POLICY_BATCH_ID_EXTRACTION_PATTERN, ".*");
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

    @Test
    public void marksLast() throws IOException, InterruptedException {
        List<Path> validPaths = new ArrayList();
        Path path;
        for (Path dir : directories) {
            path = new Path(dir, String.valueOf(System.nanoTime() + ".txt"));
            fs.createNewFile(path);
            validPaths.add(path);
            path = new Path(dir, String.valueOf(System.nanoTime() + ".txt"));
            fs.createNewFile(path);
            validPaths.add(path);
            path = new Path(dir, String.valueOf(System.nanoTime() + ".txt"));
            fs.createNewFile(path);
            validPaths.add(path);
            //this file does not match the regexp
            fs.createNewFile(new Path(dir, String.valueOf(System.nanoTime()) + ".invalid"));
        }
        //we wait till FS has registered the files
        Thread.sleep(500);

        FileMetadata metadata;
        Iterator<FileMetadata> it = policy.execute();
        metadata = it.next();
        assertTrue(validPaths.contains(new Path(metadata.getPath())));
        assertFalse(((Boolean) metadata.getOpt(BulkIncrementalPolicy.FINAL_OPT)));
        metadata = it.next();
        assertTrue(validPaths.contains(new Path(metadata.getPath())));
        assertFalse(((Boolean) metadata.getOpt(BulkIncrementalPolicy.FINAL_OPT)));
        metadata = it.next();
        assertTrue(validPaths.contains(new Path(metadata.getPath())));
        assertTrue(((Boolean) metadata.getOpt(BulkIncrementalPolicy.FINAL_OPT)));
    }
}
