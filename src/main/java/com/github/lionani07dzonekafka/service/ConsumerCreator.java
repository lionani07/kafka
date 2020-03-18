package com.github.lionani07dzonekafka.service;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.github.lionani07dzonekafka.utils.IKafkaConstants;

public class ConsumerCreator {
	
	private final KafkaConsumer<Long, String> kafkaConsumer;
	private ConsumerRecords<Long, String> records;
	
	public ConsumerCreator() {
		this(loadProperties(), new HashSet<>(Arrays.asList(IKafkaConstants.TOPIC_NAME)));
	}
	
	public ConsumerCreator(Properties props, Set<String> suscribers ) {
		this.kafkaConsumer  = new KafkaConsumer<Long, String>(props);
		this.kafkaConsumer.subscribe(suscribers);
	}	
	
	@SuppressWarnings("deprecation")
	public void readRecords(int interval, int max_no_message_found_count) throws InterruptedException {		
		
		while (true) {
			ConsumerRecords<Long, String> records =  kafkaConsumer.poll(interval);					
			records.forEach(record -> {
	              System.err.println("Record Key: " + record.key() + " Record value: " + record.value() + " Record partition: " + record.partition() + " Record offset: " + record.offset());
	           }); 
			kafkaConsumer.commitAsync();
		}		
	}

	private static Properties loadProperties() {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROCKER);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, IKafkaConstants.GROUP_ID_CONFIG);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, IKafkaConstants.MAX_POLL_RECORDS);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, IKafkaConstants.OFFSET_RESET_EARLIER);
		
		return props;
	}
	
	public ConsumerRecords<Long, String> getRecords() {
		return records;
	}
	

}
