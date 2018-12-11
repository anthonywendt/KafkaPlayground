package com.github.anthonywendt.kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    final String bootstrapServers = "127.0.0.1:9092";
    final String topic = "first_topic";

    // need account to be approved to get these
    private String consumerKey = "";
    private String consumerSecret = "";
    private String token = "";
    private String secret = "";

    // Optional: set up some followings and track terms
//        List<Long> followings = Lists.newArrayList(1234L, 566788L);
    List<String> terms = Lists.newArrayList("trump");
//        hosebirdEndpoint.followings(followings);

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

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping application...");
            logger.info("Shutting down twitter client...");
            twitterClient.stop();
            logger.info("Closing producer...");
            producer.close();
            logger.info("Shutdown complete");
        }));

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
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null){
                            logger.error("Something bad happened", e);
                        }
                    }
                });

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

        // create safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // kafka 2.0 >= 1.1 so we can keep this as 5. Otherwise use 1.

        // high throughput producer (at the expense of a bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32 KB batch size

        // create the producer
        return new KafkaProducer<>(properties);
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
//        BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

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
