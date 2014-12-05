package edu.mapred.finalproject;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlightDataGraphPartitioner extends Partitioner<Text, Text> {

	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		String source = key.toString();

		return source.hashCode() % numPartitions;
	}
}
