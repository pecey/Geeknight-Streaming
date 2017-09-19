package com.thoughtworks.geeknight.streaming.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer {
  private Properties producerProperties;
  private KafkaProducer producer;

  public Producer(){
    producerProperties = getProperties();
    producer = new KafkaProducer(producerProperties);
  }

  private Properties getProperties() {
    Properties producerProperties = new Properties();
    producerProperties.setProperty("bootstrap.servers", "localhost:9092");
    producerProperties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
    producerProperties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
    producerProperties.setProperty("group.id","kafka-producer-1");
    return producerProperties;
  }

  public void send(String topic, String value){
    producer.send(new ProducerRecord(topic, value));
  }

  public void close(){
    producer.close();
  }

  public static void main(String[] args) {

    Producer producer = new Producer();
    String topicToSendTo = "test";
    producer.send(topicToSendTo, "value");
    producer.close();
  }

}
