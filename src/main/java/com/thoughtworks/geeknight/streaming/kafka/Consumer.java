package com.thoughtworks.geeknight.streaming.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.*;

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

  private Map<String,Integer> process(){
    ConsumerRecords<String, StatusWrapper> records = consumer.poll(pollDuration);
    if(records.isEmpty()){
      return Collections.emptyMap();
    }
    Map<String,Integer> values = new HashMap<>();
    for (ConsumerRecord<String, StatusWrapper> record : records) {
     String language = record.value().getLanguage();
     if(values.containsKey(language))
       values.put(language, values.get(language) + 1);
     else
       values.put(language, 0);
    }
    return values;
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
        int timeout = 0;

        List<String> topics = Collections.singletonList("test");
        consumer.subscribe(topics);
        consumer.setPollDuration(2000);

        Map<String, Integer> languageCount = new HashMap<>();
        while (timeout < 1000) {
            Map<String, Integer> values = consumer.process();
            if (!values.isEmpty()) {
                for (String language : values.keySet()) {
                    if (languageCount.containsKey(language))
                        languageCount.put(language, languageCount.get(language) + values.get(language));
                    else
                        languageCount.put(language, values.get(language));
                }
            }
            printCountMap(languageCount);
            timeout++;
        }

        consumer.close();

    }

    private static void printCountMap(Map<String, Integer> countMap) {
        for (String key : countMap.keySet()) {
            System.out.println(key + " : " + countMap.get(key));
        }
    }
}

