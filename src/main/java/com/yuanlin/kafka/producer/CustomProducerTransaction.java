package com.yuanlin.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerTransaction
{
	public static void main(String[] args)
	{
		Properties properties = new Properties();

		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "esMaster:9092");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"firstTransactionID");

		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

		kafkaProducer.initTransactions();
		kafkaProducer.beginTransaction();

		try
		{
			for(int i = 0; i < 50; i++)
			{
				kafkaProducer.send(new ProducerRecord<>("foo","test3:"+i));

				System.out.println("finish:"+ i);
			}
		}catch(Exception e){
			kafkaProducer.abortTransaction();
		}finally
		{
			kafkaProducer.close();
			//KafkaClient k = null;
		}

	}
}
