package com.thoughtworks.geeknight.streaming.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import twitter4j.Status;

import java.util.HashMap;
import java.util.Map;

public class PrintBolt implements IRichBolt {

  OutputCollector collector;
  Map<String, Integer> languageCount;
  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    collector =  outputCollector;
    languageCount = new HashMap<>();
  }

  @Override
  public void execute(Tuple tuple) {
    String language = tuple.getValueByField("language").toString();
//    int count = Integer.getInteger(tuple.getValueByField("count").toString());
    if(languageCount.containsKey(language))
      languageCount.put(language, languageCount.get(language) + 1);
    else
      languageCount.put(language, 0);

    printLanguageMap();
  }

  private void printLanguageMap() {
    for (String language : languageCount.keySet()){
      System.out.println(language + " : " + languageCount.get(language));
    }
  }

  @Override
  public void cleanup() {

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
