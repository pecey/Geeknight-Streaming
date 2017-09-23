#Streaming your data

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
