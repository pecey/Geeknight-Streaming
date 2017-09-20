package com.thoughtworks.geeknight.streaming.storm;

import org.apache.storm.topology.IRichBolt;

public class BoltWrapper implements Comparable{
  String boltName;
  String previousNode;
  IRichBolt bolt;
  int priority;

  public BoltWrapper(String boltName, String previousNode, IRichBolt bolt, int priority){
    this.boltName = boltName;
    this.previousNode = previousNode;
    this.bolt = bolt;
    this.priority = priority;
  }

  @Override
  public int compareTo(Object o) {
    BoltWrapper other = (BoltWrapper) o;
    return new Integer(priority).compareTo(other.priority);
  }
}
