/**
 * 
 */
package edu.mapred.finalproject;

import java.io.IOException;
import java.util.ArrayList;

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
public class HADIStage1 {
	public static class HADIStage1Mapper extends
			Mapper<Object, Text, Text, Text> {
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// Get input line parts
			String line = value.toString();
			if (line != null) {
				String[] lineParts = line.split(FlightConstants.DELIMITER);
				String inputType = lineParts[0];
				String src = null;
				String dest = null;

				// Text k = null;
				// Text v = null;

				if (inputType.equals("BC")) {
					// Check if iteration 0
					String bitmaskCommand = context.getConfiguration().get(
							"bitmaskCommand");
					if (bitmaskCommand.equals("BC")) {
						src = lineParts[1];
						Text k = new Text(src);
						Text v = new Text(inputType + FlightConstants.DELIMITER);

						// Emit
						context.write(k, v);
					}
				} else if (inputType.equals("B")) {
					src = lineParts[1];
					String bitMaskStr = lineParts[2];
					Text k = new Text(src);
					Text v = new Text(inputType + FlightConstants.DELIMITER
							+ bitMaskStr);

					// Emit
					context.write(k, v);
				} else if (inputType.equals("R")) {
					src = lineParts[1];
					dest = lineParts[2];
					Text k = new Text(dest);
					Text v = new Text(inputType + FlightConstants.DELIMITER
							+ src);

					// Emit
					context.write(k, v);
				} else {
					System.err
							.println("HADI Stage 1 Mapper: Input Type Unrecognized!");
					System.exit(-2);
				}
			}
		}
	}

	public static class HADIStage1Partitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			// TODO
			System.out.println("HADIStage1Partitioner: Current Node ID = "
					+ key.toString());

			String nodeId = key.toString();
			return nodeId.hashCode() % numPartitions;
		}

	}

	public static class HADIStage1GroupComparator extends WritableComparator {

		protected HADIStage1GroupComparator() {
			super(Text.class, true);
		}

		public int compare(Text key1, Text key2) {

			return (key1.toString().compareTo(key2.toString()));
		}
	}

	public static class HADIStage1Reducer extends
			Reducer<Text, Text, NullWritable, Text> {
		ArrayList<String> listDestNodes = null;

		String sourceNode = null;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			sourceNode = new String();
			listDestNodes = new ArrayList<String>();
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			sourceNode = key.toString();
			NodeBitmask nodeBitmask = null;

			for (Text valueText : values) {
				String[] valueSplits = valueText.toString().split(
						FlightConstants.DELIMITER);
				
				//TODO for testing
				StringBuilder sb = new StringBuilder();
				for(String s : valueSplits) {
					sb.append(s + ":");
				}
				System.out.println(new String(sb));

				String inputType = valueSplits[0];
				if (inputType.equals("BC")) {
					nodeBitmask = new NodeBitmask();
				} else if (inputType.equals("B")) {
					String nbStr = valueSplits[1];
					nodeBitmask = new NodeBitmask(nbStr);
				} else if (inputType.equals("R")) {
					String node = valueSplits[1];
					if (!listDestNodes.contains(node)) {
						listDestNodes.add(node);
					}
				} else {
					System.err
							.println("HADI Stage 1 Reducer: Input Type Unrecognized!");
					System.exit(-2);
				}
			}

			NullWritable k = NullWritable.get();
			Text v = null;

			if (!listDestNodes.isEmpty()) {
				for (String destNode : listDestNodes) {
					v = new Text("B" + FlightConstants.DELIMITER + destNode
							+ FlightConstants.DELIMITER
							+ nodeBitmask.toString());
					context.write(k, v);
				}
			}

			if (!sourceNode.isEmpty() && !listDestNodes.contains(sourceNode)) {
				v = new Text("B" + FlightConstants.DELIMITER + sourceNode
						+ FlightConstants.DELIMITER + nodeBitmask.toString());
				context.write(k, v);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// NullWritable key = NullWritable.get();
			// Text value = null;
			//
			// if (!listDestNodes.isEmpty()) {
			// for (String destNode : listDestNodes) {
			// value = new Text("B" + FlightConstants.DELIMITER + destNode
			// + FlightConstants.DELIMITER + nb.toString());
			// context.write(key, value);
			// }
			// }
			//
			// if (!sourceNode.isEmpty() && !listDestNodes.contains(sourceNode))
			// {
			// value = new Text("B" + FlightConstants.DELIMITER + sourceNode
			// + FlightConstants.DELIMITER + nb.toString());
			// context.write(key, value);
			// }

			listDestNodes = null;
			// nb = null;
			sourceNode = null;
			super.cleanup(context);
		}
	}
}
