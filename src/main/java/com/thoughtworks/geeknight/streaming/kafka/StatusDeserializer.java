package com.thoughtworks.geeknight.streaming.kafka;

import org.apache.kafka.common.serialization.Deserializer;
import twitter4j.Status;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;

public class StatusDeserializer  implements Deserializer{
  @Override
  public void configure(Map map, boolean b) {

  }

  @Override
  public Status deserialize(String s, byte[] bytes) {
    try {
      ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
      ObjectInputStream objectStream = new ObjectInputStream(byteStream);

      Object o = objectStream.readObject();

      return (Status) o;
    } catch (IOException | ClassNotFoundException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public void close() {

  }
}
