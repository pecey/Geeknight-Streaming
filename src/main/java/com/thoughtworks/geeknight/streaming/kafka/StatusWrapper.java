package com.thoughtworks.geeknight.streaming.kafka;

import twitter4j.Status;

public class StatusWrapper {
  private String userid;
  private String email;
  private String language;

public StatusWrapper(){

}
  public StatusWrapper(String userid, String email, String language) {
  super();
    this.userid = userid;
    this.email = email;
    this.language = language;
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

  public StatusWrapper(Status twitterStatus){
    this.userid = twitterStatus.getUser().getName();
    this.email = twitterStatus.getUser().getEmail();
    this.language = twitterStatus.getLang();
  }

  @Override
  public String toString() {
    return "StatusWrapper{" +
        "userid='" + userid + '\'' +
        ", email='" + email + '\'' +
        '}';
  }
}
