package com.yuanlin.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

public class OffsetsManualCommitConsumer
{
	public static void main(String[] args)
	{
		Properties properties = new Properties();

		//1---info configuration
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"esMaster:9092,HWKMaster:9092");
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, "fooConsumer");
		properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, false);

		//2---deserialization
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		//3---Consumer client
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

		//4---Topic
		ArrayList<String> topics = new ArrayList<>();
		topics.add("foo2");
		kafkaConsumer.subscribe(topics);

		//5---get data
		while (true)
		{
			ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
			for(ConsumerRecord<String, String> consumerRecord:consumerRecords)
			{
				System.out.println(consumerRecord);
			}

			//commitSync
			//kafkaConsumer.commitSync();

			////commitAsync
			kafkaConsumer.commitAsync();
		}



	}
}
