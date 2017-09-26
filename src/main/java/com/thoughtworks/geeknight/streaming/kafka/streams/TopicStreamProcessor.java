package com.thoughtworks.geeknight.streaming.kafka.streams;

import java.util.Properties;

import com.thoughtworks.geeknight.streaming.kafka.StatusDeserializer;
import com.thoughtworks.geeknight.streaming.kafka.StatusSerializer;
import com.thoughtworks.geeknight.streaming.kafka.StatusWrapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

public class TopicStreamProcessor {
    public static void main(String[] args) {

        final Serializer<StatusWrapper> statusWrapperSerializer = new StatusSerializer();
        final Deserializer<StatusWrapper> statusWrapperDeserializer = new StatusDeserializer();

        final Serde<StatusWrapper> statusWrapperSerde = Serdes.serdeFrom(statusWrapperSerializer, statusWrapperDeserializer);

        KStreamBuilder builder = new KStreamBuilder();
        Properties properties = getProperties();

        KStream<String, StatusWrapper> inputDataStream = builder.stream(Serdes.String(), statusWrapperSerde,"test");
        KStream<String, String> processedDataStream = inputDataStream.mapValues(statusWrapper -> statusWrapper.getLanguage());
        processedDataStream.to(Serdes.String(), Serdes.String(), "test2");

        KafkaStreams streams = new KafkaStreams(builder, properties);
        streams.start();
    }

    private static Properties getProperties() {
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProperties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"twitter.kafka.streams.processor");
        return consumerProperties;
    }
}
