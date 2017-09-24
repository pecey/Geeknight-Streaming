package com.thoughtworks.geeknight.streaming.kafka;

import twitter4j.Status;

public class StatusWrapper {
  private String userid;
  private String email;
  private String language;
  private String tweet;

public StatusWrapper(){

}
  public StatusWrapper(String userid, String email, String language, String tweet) {
  super();
    this.userid = userid;
    this.email = email;
    this.language = language;
    this.tweet = tweet;
  }

  public String getUserid() {
    return userid;
  }

  public String getEmail() {
    return email;
  }

  public String getLanguage() {
    return language;
  }

  public String getTweet() { return tweet; }


  public StatusWrapper(Status twitterStatus){
    this.userid = twitterStatus.getUser().getName();
    this.email = twitterStatus.getUser().getEmail();
    this.language = twitterStatus.getLang();
    this.tweet = twitterStatus.getText();
  }
}
