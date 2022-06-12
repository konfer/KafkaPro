package com.yuanlin.kafka.producer;

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducer
{
	public static void main(String[] args)
	{
		Properties properties = new Properties();

		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "116.205.228.254:9092");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

		for(int i = 0; i < 5; i++)
		{
			kafkaProducer.send(new ProducerRecord<>("foo","test:"+i), new Callback(){
				@Override public void onCompletion (RecordMetadata metaData, Exception exce)
				{
					if(exce == null)
					{
						System.out.println("topic:"+ metaData.topic()+ " Partition:"+metaData.partition());
					}
				}
			});
			System.out.println("finish:"+ i);
		}

		kafkaProducer.close();
		//KafkaClient k = null;
	}
}
