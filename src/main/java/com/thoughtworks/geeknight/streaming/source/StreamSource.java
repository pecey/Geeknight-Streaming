package com.thoughtworks.geeknight.streaming.source;

import com.google.common.io.Resources;
import com.thoughtworks.geeknight.streaming.kafka.Producer;
import com.thoughtworks.geeknight.streaming.kafka.StatusWrapper;
import twitter4j.*;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class StreamSource {
  public static void main(String[] args) {
    TwitterStream twitterStream;
    twitterStream = new TwitterStreamFactory(getConfig()).getInstance();

    Producer producer = new Producer();
    String topicToSendTo = "test";

//    Spout spout = new Spout();
    twitterStream.addListener(new StatusListener() {
      @Override
      public void onStatus(Status status) {
//        spout.enqueue(status);
        producer.send(topicToSendTo, new StatusWrapper(status));
      }

      @Override
      public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

      }

      @Override
      public void onTrackLimitationNotice(int i) {

      }

      @Override
      public void onScrubGeo(long l, long l1) {

      }

      @Override
      public void onStallWarning(StallWarning stallWarning) {

      }

      @Override
      public void onException(Exception e) {

      }
    });
    twitterStream.sample();
//    Topology stormTopology = new Topology();
//    stormTopology.setSpout(spout);
//    stormTopology.setBolt(new BoltWrapper("group-by-bolt","spout",new GroupByLanguageBolt(),1));
//    stormTopology.setBolt(new BoltWrapper("print-bolt", "group-by-bolt", new PrintBolt(),2));
//    stormTopology.submit();
  }

  private static Configuration getConfig() {

    ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();

    try {
      InputStream resourceStream = Resources.getResource("config.properties").openStream();
      Properties resourceConfig = new Properties();
      resourceConfig.load(resourceStream);

      configurationBuilder.setOAuthConsumerKey(resourceConfig.getProperty("consumerKey"))
          .setOAuthConsumerSecret(resourceConfig.getProperty("consumerSecret"))
          .setOAuthAccessToken(resourceConfig.getProperty("accessToken"))
          .setOAuthAccessTokenSecret(resourceConfig.getProperty("accessTokenSecret"));


    } catch (IOException e) {
      e.printStackTrace();
    }
    return configurationBuilder.build();
  }
}
