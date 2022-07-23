package com.yuanlin.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerParameters
{
	public static void main(String[] args)
	{
		Properties properties = new Properties();

		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "esMaster:9092");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33444432);
		properties.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
		properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");


		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

		for(int i = 0; i < 50; i++)
		{
			kafkaProducer.send(new ProducerRecord<>("foo","test2:"+i), new Callback(){
				@Override public void onCompletion (RecordMetadata metaData, Exception exce)
				{
					if(exce == null)
					{
						System.out.println("topic:"+ metaData.topic()+ " Partition:"+metaData.partition());
					}
				}
			});

			try
			{
				Thread.sleep(2);
			} catch(InterruptedException e)
			{
				throw new RuntimeException(e);
			}
			System.out.println("finish:"+ i);
		}

		kafkaProducer.close();
		//KafkaClient k = null;
	}
}
