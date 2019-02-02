package com.jay.kafka.tutorial1;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithKeys {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		
		final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);
		
		String bootStrapServers = "127.0.0.1:9092";
		
		// create producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// create the producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		/**
		 * no key was provided for partition so msgs will go to random partitions.
		 * So, the data received is not ordered.
		 */
		for (int i = 0; i < 10; i++) {
			// create a producer record
			
			String topic = "first_topic";
			String value = "hello world" + Integer.toString(i);
			String key = "id_" + Integer.toString(i); // providing the key always goes to the same partition
			
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
			
			logger.info("Key : " + key);
			
			// send data -asynchronous
			producer.send(record, new Callback() {
				
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					// executed after success or exception
					if (exception == null) {
						logger.info("Received new metadata: \n" + 
									"Topic: " + metadata.topic() + "\n" + 
									"Partition: " + metadata.partition() + "\n" +
									"Offset: " + metadata.offset() + "\n" +
									"Timestamp: " + metadata.timestamp() + "\n"
									);
					}
					else {
						logger.error("Error while producing", exception);
					}
				}
			}).get(); // block the send to make it sync. DON'T DO THIS IN PRODUCTION
		}
		// flush and close producer
		producer.flush();
		producer.close();
	}
}
