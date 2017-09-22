package com.thoughtworks.geeknight.streaming.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;
public class StatusSerializer implements Serializer<StatusWrapper> {
  @Override
  public void configure(Map map, boolean b) {

  }

  @Override
  public byte[] serialize(String s, StatusWrapper status) {
    byte[] retVal = null;
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      retVal = objectMapper.writeValueAsString(status).getBytes();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return retVal;
  }

  @Override
  public void close() {

  }
}
