package com.jay.kafka.tutorial3;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;

public class ElasticSearchConsumer {

	public static RestHighLevelClient createClient() {
		
		String hostname = "kafka-course-1058327939.ap-southeast-2.bonsaisearch.net";
		String username = "82d9np64s3";
		String password = "30457xynt0";
		
		// need for bonsai ES
		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
	
		RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
										.setHttpClientConfigCallback(new HttpClientConfigCallback() {
											
											@Override
											public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
												// TODO Auto-generated method stub
												return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
											}
										});
		RestHighLevelClient client = new RestHighLevelClient(builder);
		return client;
	}
	
	public static KafkaConsumer<String, String> createConsumer(String topic) {
		String bootStrapServers = "127.0.0.1:9092";
		String groupId = "kafka-demo-elasticsearch"; // changing groupId resets the application
		
		// create consumer configs
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		// create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);	
		consumer.subscribe(Arrays.asList(topic));
		
		return consumer;
	}
	
	public static void main (String[] args) throws IOException {
		Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
		
		RestHighLevelClient client = createClient();
		
		//String jsonString = "{ \"foo\" : \"bar\" }";
				
		KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");
		
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0
			for (ConsumerRecord<String, String> record : records) {
				// insert data into ES
				String jsonString = record.value();
				
				// 2 strategies
				// kafka generic Id - unique
				//String id = record.topic() + "_" + record.partition() + "_" + record.offset();
				// twitter feed specific id and use it in IndexRequest
				String id = extractIdFromTweet (record.value());
				
				IndexRequest indexRequest = new IndexRequest(
						"twitter", 
						"tweets",
						id // this is to make idempotent consumer
						).source(jsonString, XContentType.JSON);
				
				IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
				logger.info(indexResponse.getId());
				try {
					Thread.sleep (1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		// close the client gracefully
		//client.close();
	}

	
	private static JsonParser jsonParser = new JsonParser();
	private static String extractIdFromTweet(String tweetJson) {
		// gson
		return jsonParser.parse(tweetJson)
				.getAsJsonObject()
				.get("id_str")
				.getAsString();
	}
}
