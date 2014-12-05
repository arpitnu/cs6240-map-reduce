/**
 * 
 */
package edu.mapred.finalproject;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author arpitm
 * 
 */
public class HADIStage2 {
	public static class HADIStage2Mapper extends
			Mapper<Object, Text, Text, Text> {
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			if (value != null) {
				String[] lineSplits = value.toString().split(
						FlightConstants.DELIMITER);

				String inputType = lineSplits[0];
				if (inputType.equals("B")) {
					String node = lineSplits[1];
					String bitmaskStr = lineSplits[2];

					Text k = new Text(node);
					Text v = new Text(inputType + FlightConstants.DELIMITER
							+ bitmaskStr);

					// Emit
					context.write(k, v);
				}
			}
		}
	}

	public static class HADIStage2Partitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			String nodeId = key.toString();
			return (nodeId.hashCode() % numPartitions);
		}

	}

	public static class HADIStage2GroupComparator extends WritableComparator {

		protected HADIStage2GroupComparator() {
			super(Text.class, true);
		}

		public int compare(Text key1, Text key2) {

			return (key1.toString().compareTo(key2.toString()));
		}
	}

	public static class HADIStage2Reducer extends
			Reducer<Text, Text, NullWritable, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String node = key.toString();

			// Create a new FMBitmask for the node for the current iteration
			NodeBitmask currHopBitmask = new NodeBitmask();
			currHopBitmask.setBit(node.hashCode()
					% FlightConstants.TOTAL_NUM_NODES);

			String inputType = null;

			for (Text value : values) {
				String[] valueSplits = value.toString().split(
						FlightConstants.DELIMITER);
				inputType = valueSplits[0];

				// Old bitmask for the node
				NodeBitmask prevHopBitmask = new NodeBitmask(valueSplits[1]);

				// Bitwise OR with new FMBitmask
				currHopBitmask.bitwiseOrWith(prevHopBitmask);
			}

			// Define key and value to emit
			NullWritable k = NullWritable.get();
			Text v = new Text(inputType + FlightConstants.DELIMITER + node
					+ FlightConstants.DELIMITER + currHopBitmask.toString());

			// Emit
			context.write(k, v);
		}
	}
}
