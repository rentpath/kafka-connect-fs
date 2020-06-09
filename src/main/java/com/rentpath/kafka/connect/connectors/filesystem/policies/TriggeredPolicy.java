package com.rentpath.kafka.connect.connectors.filesystem.policies;

import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import com.github.mmolimar.kafka.connect.fs.policy.AbstractPolicy;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

public class TriggeredPolicy extends AbstractPolicy {
    public static final String TRIGGERED_POLICY_PREFIX = FsSourceTaskConfig.POLICY_PREFIX + "triggered.";
    public static final String CONSUMER_BOOTSTRAP_SERVERS = TRIGGERED_POLICY_PREFIX + "bootstrap.servers";
    public static final String CONSUMER_GROUP_ID_PREFIX = TRIGGERED_POLICY_PREFIX + "group.id.prefix";
    public static final String CONSUMER_KEY_DESERIALIZER_CLASS = TRIGGERED_POLICY_PREFIX + "key.deserializer.class";
    public static final String CONSUMER_VALUE_DESERIALIZER_CLASS = TRIGGERED_POLICY_PREFIX + "value.deserializer.class";
    public static final String CONSUMER_TOPIC = TRIGGERED_POLICY_PREFIX + "trigger.topic";
    public static final String CONSUMER_POLL_DURATION_MS = TRIGGERED_POLICY_PREFIX + "poll.duration.ms";

    private Consumer<Void, String> consumer;
    private String fileRegexp;
    private Duration pollDuration;

    public TriggeredPolicy(FsSourceTaskConfig conf) throws IOException {
        super(conf);
        fileRegexp = conf.getString(FsSourceTaskConfig.POLICY_REGEXP);
    }

    @Override
    protected void configPolicy(Map<String, Object> conf) {
        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, conf.get(CONSUMER_BOOTSTRAP_SERVERS));
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, ((String) conf.get(CONSUMER_GROUP_ID_PREFIX)) + "--" + fileRegexp);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, conf.get(CONSUMER_KEY_DESERIALIZER_CLASS));
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, conf.get(CONSUMER_VALUE_DESERIALIZER_CLASS));
        consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        try {
            pollDuration = Duration.ofMillis((Long.parseLong((String) conf.get(CONSUMER_POLL_DURATION_MS))));
        } catch (NumberFormatException e) {
            throw new ConfigException(CONSUMER_POLL_DURATION_MS + " property is required and must be a number(long). Got: " +
                    conf.get(CONSUMER_POLL_DURATION_MS));
        }
        consumer = new KafkaConsumer<Void, String>(consumerProperties);
        consumer.assign(Collections.singletonList(new TopicPartition((String) conf.get(CONSUMER_TOPIC), 1)));
    }

    @Override
    protected boolean isPolicyCompleted() {
        return false;
    }

    @Override
    protected void preCheck() {
        while (true) {
            ConsumerRecords<Void, String> records = consumer.poll(pollDuration);
            if (!records.isEmpty()) {
                break;
            }
        }
    }
}
