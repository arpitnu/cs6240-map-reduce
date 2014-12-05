/**
 * 
 */
package edu.mapred.finalproject;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author arpitm
 * 
 */
public class HADIStage3 {
	public static class HADIStage3Mapper extends
			Mapper<Object, Text, Text, IntWritable> {
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			if (value != null) {
				String[] lineSplits = value.toString().split(
						FlightConstants.DELIMITER);
				String inputType = lineSplits[0];

				// Key & value of mapper
				Text k = null;
				IntWritable v = null;

				if (inputType.equals("B")) {
					// String node = lineSplits[1];
					String bitmaskStr = lineSplits[2];

					NodeBitmask currHopBitmask = new NodeBitmask(bitmaskStr);
					int b = currHopBitmask.getSetBitsCount();

					// Calculate N(h)
					int nodeCurrhopN = b;

					k = new Text("N");
					v = new IntWritable(nodeCurrhopN);

					// Emit
					context.write(k, v);
				}
			}
		}
	}

	public static class HADIStage3Partitioner extends
			Partitioner<Text, IntWritable> {

		@Override
		public int getPartition(Text key, IntWritable value, int numPartitions) {
			return 0;
		}

	}

	public static class HADIStage3Reducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			// Aggregate all nodes Neighborhood functions.
			int allNodeCurrHopN = 0;
			String currHop = context.getConfiguration().get("currHop");

			if (key.toString().equals("N")) {
				for (IntWritable value : values) {
					allNodeCurrHopN += value.get();
				}

				// Key and value for emit
				Text k = new Text("N at hop " + currHop + " is");
				IntWritable v = new IntWritable(allNodeCurrHopN);

				// Update counter
				context.getCounter(
						FlightConstants.FlightDataGraphCounters.PREV_HOP_N_COUNTER)
						.setValue((long) allNodeCurrHopN);

				// Emit
				context.write(k, v);
			}
		}
	}
}
