package com.thoughtworks.geeknight.streaming.source;

import com.thoughtworks.geeknight.streaming.kafka.Producer;
import twitter4j.*;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import com.google.common.io.Resources;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class StreamSource {
  public static void main(String[] args) {
    TwitterStream twitterStream;
    twitterStream = new TwitterStreamFactory(getConfig()).getInstance();

    Producer producer = new Producer();
    String topicToSendTo = "test";

    twitterStream.addListener(new StatusListener() {
      @Override
      public void onStatus(Status status) {
        producer.send(topicToSendTo, status.getText());
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
