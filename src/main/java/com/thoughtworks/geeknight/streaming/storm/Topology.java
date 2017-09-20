package com.thoughtworks.geeknight.streaming.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.Bolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.TopologyBuilder;

import java.io.IOException;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.Queue;

public class Topology {

  Queue<BoltWrapper> bolts = new PriorityQueue<>();
  Spout spout;

  public Topology(){

  }

  public void setSpout(Spout spout){
    this.spout = spout;
  }

  public void setBolt(BoltWrapper bolt){
    this.bolts.add(bolt);
  }

  public void submit(){
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("spout", spout);
    for (BoltWrapper wrapper:bolts) {
      builder.setBolt(wrapper.boltName, wrapper.bolt).shuffleGrouping(wrapper.previousNode);
    }
    Config conf = new Config();
    conf.setDebug(false);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("twitterStorm", conf, builder.createTopology());
  }
}

