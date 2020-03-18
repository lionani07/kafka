package com.github.lionani07dzonekafka;

import java.util.Date;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.github.lionani07dzonekafka.model.Cliente;
import com.github.lionani07dzonekafka.service.ConsumerCreator;
import com.github.lionani07dzonekafka.service.ProducerCreator;
import com.github.lionani07dzonekafka.utils.IKafkaConstants;

@SpringBootApplication
public class DzoneKafkaApplication {

	public static void main(String[] args) {
		System.out.println("Inciando app...");		
		runProducer();
		runConsumer();
	}
	
	public static void runProducer() {
		ProducerCreator producer = new ProducerCreator();
		RecordMetadata metadata;
		for (int i = 0; i < IKafkaConstants.MESSAGE_COUNT; i++) {			
			try {
				metadata = producer.sendMessage(IKafkaConstants.TOPIC_NAME, new Cliente("Liorge", 34, new Date())).get();
				System.out.printf("Record send with key %d to partition %d with offset %d \n", i, metadata.partition(), metadata.offset());
			} catch (InterruptedException | ExecutionException e) {				
				e.printStackTrace();
			}				
		}		
	}
	
	public static void runConsumer() {
		ConsumerCreator consumerCreator = new ConsumerCreator();
		try {
			consumerCreator.readRecords(1000, IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT);
		} catch (InterruptedException e) {			
			e.printStackTrace();
		}
	}
	
	

}
