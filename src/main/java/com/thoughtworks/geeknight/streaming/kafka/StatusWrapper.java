package com.thoughtworks.geeknight.streaming.kafka;

import twitter4j.HashtagEntity;
import twitter4j.Status;

import java.util.ArrayList;
import java.util.List;

public class StatusWrapper {

    private List<String> hashtags;
    private String country;
    private String userid;
    private String email;
    private String language;
    private String tweet;
    private int retweetCount;

    public StatusWrapper() {
    }

    public StatusWrapper(String userid, String email, String language, String tweet, int retweetCount, String country, HashtagEntity[] hashtagEntities) {
        super();
        this.userid = userid;
        this.email = email;
        this.language = language;
        this.tweet = tweet;
        this.retweetCount = retweetCount;
        this.country = country;
        hashtags = new ArrayList<>();
        for (HashtagEntity hashtagEntity : hashtagEntities) {
            hashtags.add(hashtagEntity.getText());
        }

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

    public String getTweet() {
        return tweet;
    }

    public String getCountry() {
        return country;
    }

    public int getRetweetCount() {
        return retweetCount;
    }

    public List<String> getHashtags() {
        return hashtags;
    }

    public StatusWrapper(Status twitterStatus) {
        this.userid = twitterStatus.getUser().getName();
        this.email = twitterStatus.getUser().getEmail();
        this.language = twitterStatus.getLang();
        this.tweet = twitterStatus.getText();
        this.retweetCount = twitterStatus.getRetweetCount();
        this.country = twitterStatus.getPlace().getCountry();
        hashtags = new ArrayList<>();
        for (HashtagEntity hashtagEntity : twitterStatus.getHashtagEntities()) {
            hashtags.add(hashtagEntity.getText());
        }
    }
}
