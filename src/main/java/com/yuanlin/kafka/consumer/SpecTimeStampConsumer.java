package com.yuanlin.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class SpecTimeStampConsumer
{
	public static void main(String[] args)
	{
		Properties properties = new Properties();

		//1---info configuration
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"esMaster:9092,HWKMaster:9092");
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, "fooConsumer");

		//2---deserialization
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		//3---Consumer client
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

		//4---Topic
		ArrayList<String> topics = new ArrayList<>();
		topics.add("HWCluster");
		kafkaConsumer.subscribe(topics);

		Set<TopicPartition> assignment = kafkaConsumer.assignment();

		while(assignment.size() == 0)
		{
			kafkaConsumer.poll(Duration.ofSeconds(1));
			assignment = kafkaConsumer.assignment();
		}

		//5---init timeOffset for 1 day and par hashMap
		HashMap<TopicPartition, Long> topicPartitionLongHashMap = new HashMap<>();

		for(TopicPartition par:assignment)
		{
			topicPartitionLongHashMap.put(par, System.currentTimeMillis() - 1 * 24 * 3600 * 1000);
		}

		//6---get the par and timeOffset hashMap
		Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampMap = kafkaConsumer.offsetsForTimes(topicPartitionLongHashMap);

		//7---consumer by the par and timeOffsetStamp
		for(TopicPartition par:assignment)
		{
			OffsetAndTimestamp offsetAndTimestamp = topicPartitionOffsetAndTimestampMap.get(par);
			kafkaConsumer.seek(par, offsetAndTimestamp.offset());
		}

		//8---get data
		while (true)
		{
			ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
			for(ConsumerRecord<String, String> consumerRecord:consumerRecords)
			{
				System.out.println(consumerRecord);
			}
		}

	}
}
