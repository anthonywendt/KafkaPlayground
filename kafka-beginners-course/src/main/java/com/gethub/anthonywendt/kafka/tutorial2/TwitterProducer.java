package com.gethub.anthonywendt.kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    final String bootstrapServers = "127.0.0.1:9092";
    final String topic = "first_topic";

    // need account to be approved to get these
    private String consumerKey = "LfgU0GqJeANr5Yt3RaL6mbXSY";
    private String consumerSecret = "HnXgpm7GFXd4aaMZ40tTPEJiuk3fwsulCFJLoz0YxH8EJSouwP";
    private String token = "1062415679255539717-bS27FVoEB6abWyJtoK8bCFhIt0xyxk";
    private String secret = "pHCJlmw8EmgNDISF83alutNy2O3GvBClAemSi28awFMt7";

    public TwitterProducer() {
        // empty constructor
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {

        logger.info("Setup");

        // create a twitter client
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        Client twitterClient = createTwitterClient(msgQueue);
        twitterClient.connect();

        // create a kafka producer
        Producer<String, String> producer = createProducer();

        // loop to send tweets to kafka
        while(!twitterClient.isDone()){
            String msg = null;
            try {
                 msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                twitterClient.stop();
            }
            if(msg != null) {
                logger.info(msg);

//                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, msg);
//                producer.send(producerRecord);
//                producer.flush();

//                List<String> messageKeyValues = Arrays.asList(msg.split(","));
//                List<String> textList = messageKeyValues.stream().filter(m -> m.split(":")[0].equals("\"text\"")).collect(Collectors.toList());
//                if(!textList.isEmpty()) {
////                    logger.info(textList.get(0));
//                    String message = textList.get(0);
//                    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, message);
//                    producer.send(producerRecord);
//                    producer.flush();
//                }
            }
        }
        producer.close();

        logger.info("End of application");
    }

    public Producer<String, String> createProducer() {

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        return new KafkaProducer<>(properties);
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
//        BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
//        List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList("trump");
//        hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;

    }
}
