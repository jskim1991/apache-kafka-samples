package com.jay.kafka.tutorial1;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithThread {

	private ConsumerDemoWithThread() {
		
	}
	
	private void run() {
		final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
		String bootStrapServers = "127.0.0.1:9092";
		String groupId = "my-sixth-application"; // changing groupId resets the application
		String topic = "first_topic";
		
		// latch for dealing with multiple threads
		CountDownLatch latch = new CountDownLatch(1);
		
		// create the consumer runnable
		logger.info("Creating the consumer thread");
		Runnable myConsumerRunnable = new ConsumerRunnable(bootStrapServers, groupId, topic, latch);
	
		// start the thread
		Thread myThread = new Thread(myConsumerRunnable);
		myThread.start();
		
		// add a shutdown hook - register some actions on termination
		Runtime.getRuntime().addShutdownHook(new Thread( () -> {
				logger.info("Caught shutdown hook");
				((ConsumerRunnable) myConsumerRunnable).shutdown();
				try {
					latch.await();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				logger.info("Application has exited");
			}
			));
		
		try {
			latch.await();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			logger.error("Application got interrupted", e);
		} finally {
			logger.info("Application is closing");
		}
	}
	
	public static void main(String[] args) {
		new ConsumerDemoWithThread().run();
	}
	
	public class ConsumerRunnable implements Runnable {
		
		private CountDownLatch latch;
		private KafkaConsumer<String, String> consumer;
		
		private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class); 

		
		public ConsumerRunnable(String bootStrapServers, String groupId, String topic, CountDownLatch latch) {
			this.latch = latch;
			Properties properties = new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			consumer = new KafkaConsumer<String, String>(properties);	
			consumer.subscribe(Arrays.asList(topic));
		}
		
		@Override
		public void run() {
			// poll for new data
			try {
				while(true) {
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0
					for (ConsumerRecord<String, String> record : records) {
						logger.info("Key : " + record.key() + ", Value: " + record.value());
						logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
					}
				}
			} catch(WakeupException e) {
				logger.info("Received shutdown signal!");
			} finally {
				consumer.close();
				// tell the main code that we are done with the consumer
				latch.countDown();
			}
		}
		
		public void shutdown() {
			// wakeup() is a special method to interrupt consumer.poll()
			// will throw WakeupException
			consumer.wakeup();
		}	
	}
}


