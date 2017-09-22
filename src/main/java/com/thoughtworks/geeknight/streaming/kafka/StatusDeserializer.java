package com.thoughtworks.geeknight.streaming.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class StatusDeserializer  implements Deserializer<StatusWrapper>{
  @Override
  public void configure(Map map, boolean b) {

  }

  @Override
  public StatusWrapper deserialize(String s, byte[] bytes) {
    ObjectMapper mapper = new ObjectMapper();
    StatusWrapper status = null;
    try {
      status = mapper.readValue(bytes, StatusWrapper.class);
    } catch (Exception e) {

      e.printStackTrace();
    }
    return status;
  }

  @Override
  public void close() {

  }
}
