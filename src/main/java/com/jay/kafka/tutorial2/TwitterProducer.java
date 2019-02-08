package com.jay.kafka.tutorial2;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class TwitterProducer {

	Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
	
	String consumerKey = "71TDVzRzCcaZ4HWMf7MovEUsX";
	String consumerSecret = "pA1enVugMc4QrSaD9FPyPOYs4sbZMnbXnp1cMmDFgRCPKMxqpm";
	String token = "1093484859413032960-E1hawPK8aGb0yaAGxiOcODQUKnGMCX";
	String secret = "LAmFqFMOOzhcRz4TcQHDjJznIgIlCVMuszP3RZmu5iBg9";
	
	List<String> terms = Lists.newArrayList("kafka", "java");

	
	public TwitterProducer() {}
	
	public void run() {
		
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
		
		// create a twitter client
		Client client = createTwitterClient(msgQueue);
		// Attempts to establish a connection
		client.connect();
		
		// create kafka producer
		KafkaProducer<String, String> producer = createKafkaProducer();
		
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			client.stop();
			producer.close(); // flush
		}));
		
		// loop to send tweets to kafka	
		// on a different thread, or multiple different threads....
		while (!client.isDone()) {
			String msg = null;
		    try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				client.stop();
			}
			if (msg != null) {
			    logger.info(msg);
			    // kafka-topics.bat --zookeeper 127.0.0.1:2181 --create --topic twitter_tweets --partitions 6 --replication-factor 1
			    producer.send(new ProducerRecord<String, String>("twitter_tweets", null, msg), new Callback() {
			    	@Override
			    	public void onCompletion(RecordMetadata recordMetadata, Exception e) {
			    		if (e != null) {
			    			logger.error("Something bad happened", e);
			    		}
			    	}
			    });
			}
		}
		
		logger.info("End of app");
	}
	
	public Client createTwitterClient(BlockingQueue<String> msgQueue) {
		
		/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		
		// Optional: set up some followings and track terms
		hosebirdEndpoint.trackTerms(terms);
		
		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1("consumerKey", "consumerSecret", "token", "secret");
	
		ClientBuilder builder = new ClientBuilder()
				  .name("Hosebird-Client-01")                           // optional: mainly for the logs
				  .hosts(hosebirdHosts)
				  .authentication(hosebirdAuth)
				  .endpoint(hosebirdEndpoint)
				  .processor(new StringDelimitedProcessor(msgQueue));

		Client hosebirdClient = builder.build();
				
		return hosebirdClient;
	}
	
	public KafkaProducer<String, String> createKafkaProducer() {
		String bootStrapServers = "127.0.0.1:9092";
		
		// create producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// create safe producer
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
		
		// high throughput producer at the expense of a bit of latency and CPU usage (compression)
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
		
		// create the producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		return producer;
	}

	public static void main(String[] args) {
		new TwitterProducer().run();
	}
}
