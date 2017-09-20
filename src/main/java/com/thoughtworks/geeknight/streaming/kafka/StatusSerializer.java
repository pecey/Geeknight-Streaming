package com.thoughtworks.geeknight.streaming.kafka;

import org.apache.kafka.common.serialization.Serializer;
import twitter4j.Status;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;

public class StatusSerializer implements Serializer {
  @Override
  public void configure(Map map, boolean b) {

  }

  public byte[] serialize(String s, Status o) {

    try {
      ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
      ObjectOutputStream objectStream = new ObjectOutputStream(byteStream);
      objectStream.writeObject(o);
      objectStream.close();
      return byteStream.toByteArray();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return new byte[0];
  }

  @Override
  public byte[] serialize(String s, Object o) {
    return new byte[0];
  }

  @Override
  public void close() {

  }
}
