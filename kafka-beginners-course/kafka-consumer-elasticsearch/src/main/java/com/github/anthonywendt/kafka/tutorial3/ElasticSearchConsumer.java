package com.github.anthonywendt.kafka.tutorial3;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {
    public static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
    private static JsonParser jsonParser = new JsonParser();

    public static RestHighLevelClient createClient() {

        String hostname = "localhost";
        String username = "";
        String password = "";

        // don't do if you run a local ES
//        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
//        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 9200, "http")
        );

        return new RestHighLevelClient(builder);

    }

    public static KafkaConsumer<String, String> createConsumer(String topic) {
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // This means I need to commit the offset
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }

    public static void main(String[] args) throws IOException {

        RestHighLevelClient client = createClient();

//        System.out.println(id);

        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

        // poll for new data
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            int recordCount = records.count();
            logger.info("Received " + recordCount + " records.");

            BulkRequest bulkRequest = new BulkRequest();

            for(ConsumerRecord<String, String> record: records) {

                // 2 strategies
                // kafka generic ID
//                String id = record.topic() + "_" + record.partition() + "_" + record.offset();
                try {
                    // twitter feed specific ID
                    String tweetId = extractIdFromTweet(record.value());
                    // where we instert data into ElasticSearch
                    IndexRequest indexRequest = new IndexRequest("twitter", "tweets", tweetId).source(record.value(), XContentType.JSON);

                    bulkRequest.add(indexRequest); // we add to our bulk request (takes no time)
//                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
//                String id = indexResponse.getId();
//                logger.info(id);

//                try {
//                    Thread.sleep(1000); // introduce a small delay
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
                } catch (NullPointerException e) {
                    logger.warn("Received bad data... skipping : " + record.value());
                }
            }
            if(recordCount > 0) {
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Committing offsets...");
                consumer.commitSync();
                logger.info("Offsets have been committed");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        // close the client gracefully
//        client.close();

    }

    private static String extractIdFromTweet(String tweetJson) {
        // gson library

        return jsonParser.parse(tweetJson).getAsJsonObject().get("id_str").getAsString();
    }
}
