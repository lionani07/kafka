package com.github.lionani07dzonekafka.service;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;

import com.github.lionani07dzonekafka.model.Cliente;
import com.github.lionani07dzonekafka.utils.IKafkaConstants;



public class ProducerCreator {
	
	KafkaProducer<Long, Cliente> kafkaProducer;
	
	public ProducerCreator() {
		this(loadProperties());
	}
	
	public ProducerCreator(Properties props) {
		this.kafkaProducer = new KafkaProducer<Long, Cliente>(props);
	}
	
	public Future<RecordMetadata> sendMessage(String topic, Cliente cliente) {
		return this.kafkaProducer.send(new ProducerRecord<Long, Cliente>(topic, cliente));
	}
	
	
	private static Properties loadProperties() {
		Properties props = new Properties();		
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROCKER);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, IKafkaConstants.CLIENT_ID);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		props.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Cliente.class.getName());
		return props;
	}

}
