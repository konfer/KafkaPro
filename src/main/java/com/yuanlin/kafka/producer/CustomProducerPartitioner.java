package com.yuanlin.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerPartitioner
{
	public static void main(String[] args)
	{
		Properties properties = new Properties();

		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "esMaster:9092");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

		for(int i = 0; i < 5; i++)
		{
			kafkaProducer.send(new ProducerRecord<>("HWCluster",1,"","test1:"+i), new Callback(){
				@Override
				public void onCompletion (RecordMetadata metaData, Exception exce)
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
