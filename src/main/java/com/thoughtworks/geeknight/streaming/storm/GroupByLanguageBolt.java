package com.thoughtworks.geeknight.streaming.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import twitter4j.Status;

import java.util.Map;

public class GroupByLanguageBolt implements IRichBolt {

  OutputCollector collector;
  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    collector = outputCollector;
  }

  @Override
  public void execute(Tuple tuple) {
    Status tweet = (Status) tuple.getValueByField("tweet");
    collector.emit(tuple, new Values(tweet.getLang(), "1"));
    collector.ack(tuple);
  }

  @Override
  public void cleanup() {

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("language","count"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
