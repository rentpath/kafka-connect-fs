package com.github.mmolimar.kafka.connect.fs.file.reader;

import java.util.function.Function;

@FunctionalInterface
public interface ReaderAdapter<T> extends Function<T, org.apache.kafka.connect.data.SchemaAndValue> {
}
