package com.github.mmolimar.kafka.connect.fs.util;

import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import com.github.mmolimar.kafka.connect.fs.file.reader.FileReader;
import com.github.mmolimar.kafka.connect.fs.file.reader.schema.SchemaReader;
import com.github.mmolimar.kafka.connect.fs.policy.Policy;
import org.apache.commons.lang.reflect.ConstructorUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Map;

public class ReflectionUtils {

    public static FileReader makeReader(Class<? extends FileReader> clazz, FileSystem fs,
                                        Path path, Map<String, Object> config) throws Throwable {
        return make(clazz, fs, path, config);
    }

    public static Policy makePolicy(Class<? extends Policy> clazz, FsSourceTaskConfig conf) throws Throwable {
        return make(clazz, conf);
    }

    public static SchemaReader makeSchemaReader(FileSystem fs, Class<? extends SchemaReader> clazz, Map<String, Object> config) throws Throwable {
        return make(clazz, fs, config);
    }

    private static <T> T make(Class<T> clazz, Object... args) throws Throwable {
        try {
            Class[] constClasses = Arrays.stream(args).map(arg -> arg.getClass()).toArray(Class[]::new);

            Constructor constructor = ConstructorUtils.getMatchingAccessibleConstructor(clazz, constClasses);
            return (T) constructor.newInstance(args);
        } catch (IllegalAccessException |
                InstantiationException |
                InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
