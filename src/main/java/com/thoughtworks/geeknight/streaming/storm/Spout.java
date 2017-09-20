package com.thoughtworks.geeknight.streaming.storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import twitter4j.Status;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class Spout implements IRichSpout {
  private SpoutOutputCollector collector;
  private LinkedBlockingQueue<Status> queue;

  public Spout(){
    queue = new LinkedBlockingQueue<>(1000);
  }

  @Override
  public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
    collector = spoutOutputCollector;
  }

  @Override
  public void close() {

  }

  @Override
  public void activate() {

  }

  @Override
  public void deactivate() {

  }

  @Override
  public void nextTuple() {
    Status ret = queue.poll();
    if (ret == null) {
      Utils.sleep(50);
    } else {
      collector.emit(new Values(ret));
    }
  }

  @Override
  public void ack(Object o) {

  }

  @Override
  public void fail(Object o) {

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("tweet"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }

  public void enqueue(Status status) {
    queue.offer(status);
  }
}
