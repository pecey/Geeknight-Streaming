package com.thoughtworks.geeknight.streaming.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.*;

public class Consumer {
  private KafkaConsumer<String, String> consumer;
  private Properties consumerProperties;
  private int pollDuration;

  public Consumer(){
    consumerProperties = getProperties();
    consumer = new KafkaConsumer<>(consumerProperties);
    pollDuration = 1000;
  }

  public void setPollDuration(int duration){
    pollDuration = duration;
  }

  public void subscribe(List<String> topics){
    consumer.subscribe(topics);
  }

  public void close(){
    consumer.close();
  }

  public List<String> process(){
    ConsumerRecords<String, String> records = consumer.poll(pollDuration);
    if(records.isEmpty()){
      return Collections.emptyList();
    }
    List<String> values = new ArrayList<>();
    for (ConsumerRecord<String, String> record : records) {
     values.add(record.value());
    }
    return values;
  }


  private static Properties getProperties() {
    Properties consumerProperties = new Properties();
    consumerProperties.setProperty("bootstrap.servers", "localhost:9092");
    consumerProperties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
    consumerProperties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
    consumerProperties.setProperty("group.id","kafka-consumer-1");
    return consumerProperties;
  }

  public static void main(String[] args) {

    Consumer consumer = new Consumer();
    int timeout = 0;

    List<String> topics = Arrays.asList("test");
    consumer.subscribe(topics);
    consumer.setPollDuration(2000);


    while(timeout < 1000){
      List<String> values = consumer.process();
      if(!values.isEmpty()){
        for (String value: values) {
          System.out.println(value);
        }
      }
      timeout ++;
    }

    consumer.close();

  }
}

