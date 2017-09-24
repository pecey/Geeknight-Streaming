# Streaming your data

#### Visualisation

We will be using redis to store counts. `redis-visualize` is a package which gives us a dashboard on topic of redis.

Run `npm install -g redis-visualize` to install the package.

To start, `redis-visualize`. By default, it will start on port 8079.

### Configuration
This project streams data using the Twitter API.

It reads the credentials from `src/resources/config.properties`. 

<pre>
 consumerKey=your-consumer-key
 consumerSecret=your-consumer-secret
 accessToken=your-access-token
 accessTokenSecret=your-access-token-secret
</pre> 

### Kafka

* Download [Kafka](https://kafka.apache.org/downloads)
* Start Zookeeper : `bin/zookeeper-server-start.sh config/zookeeper.properties`
* Start Kafka server : `bin/kafka-server-start.sh config/server.properties`
