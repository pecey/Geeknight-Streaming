package com.thoughtworks.geeknight.streaming.kafka;

import com.thoughtworks.geeknight.streaming.redis.RedisConnector;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class Consumer {
    private KafkaConsumer<String, StatusWrapper> consumer;
    private int pollDuration;

    private Consumer() {
        Properties consumerProperties = getProperties();
        consumer = new KafkaConsumer<>(consumerProperties);
        pollDuration = 1000;
    }

    private void setPollDuration(int duration) {
        pollDuration = duration;
    }

    private void subscribe(List<String> topics) {
        consumer.subscribe(topics);
    }

    private void close() {
        consumer.close();
    }

    private void process() {
        ConsumerRecords<String, StatusWrapper> records = consumer.poll(pollDuration);
        if (!records.isEmpty()) {
            RedisConnector connector = new RedisConnector();
            records.iterator().forEachRemaining(record -> connector.increment(record.value().getLanguage()));
        }
    }


    private static Properties getProperties() {
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty("bootstrap.servers", "localhost:9092");
        consumerProperties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.setProperty("value.deserializer", "com.thoughtworks.geeknight.streaming.kafka.StatusDeserializer");
        consumerProperties.setProperty("group.id", "kafka-consumer-1");
        return consumerProperties;
    }

    public static void main(String[] args) {

        Consumer consumer = new Consumer();

        List<String> topics = Collections.singletonList("test");
        consumer.subscribe(topics);
        consumer.setPollDuration(2000);

        while (true) {
            consumer.process();
        }
    }
}

