package com.github.mmolimar.kafka.connect.fs.file.reader.schema;

import org.apache.hadoop.fs.FileSystem;
import org.kohsuke.github.*;

import java.io.IOException;
import java.util.Map;

import static com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig.FILE_READER_PREFIX;

public class GithubSchemaReader implements SchemaReader {
    public static final String SCHEMA_READER_PREFIX = FILE_READER_PREFIX + "schema.reader.";
    public static final String AUTH_TOKEN = SCHEMA_READER_PREFIX + "auth.token";
    public static final String REPO = SCHEMA_READER_PREFIX + "repo";
    public static final String PATH = SCHEMA_READER_PREFIX + "path";
    public static final String TAG = SCHEMA_READER_PREFIX + "tag";

    private String schema;
    private String repoName;
    private String authToken;
    private String filepath;
    private GitHub github;
    private GHRepository repo;
    private String tag;


    public GithubSchemaReader(FileSystem fs, Map<String, Object> config) throws IOException {
        repoName = (String)config.get(REPO);
        authToken = (String)config.get(AUTH_TOKEN);
        filepath = (String)config.get(PATH);
        tag = (String) config.get(TAG);
        github = new GitHubBuilder().withOAuthToken(authToken).build();
        repo = github.getRepository(repoName);
    }

    @Override
    public String readSchema() throws IOException {
        if (schema != null)
            return schema;
        GHContent content = repo.getFileContent(filepath, tag);
        schema = content.getContent();
        return schema;
    }
}
