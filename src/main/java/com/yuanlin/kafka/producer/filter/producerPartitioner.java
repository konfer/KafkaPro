package com.yuanlin.kafka.producer.filter;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class producerPartitioner implements Partitioner
{
	@Override public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster)
	{
		String msgValues = o1.toString();
		int partition;

		if(msgValues.contains("test1"))
		{
			partition = 0;
		}else if(msgValues.contains("test2"))
		{
			partition = 1;
		}else{
			partition = 2;
		}

		return partition;
	}

	@Override public void close()
	{

	}

	@Override public void configure(Map<String, ?> map)
	{

	}
}
