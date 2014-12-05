package edu.mapred.finalproject;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlightDataGraphReducer extends
		Reducer<Text, Text, NullWritable, Text> {
	HashMap<String, HashMap<String, Integer>> graphEdgesMap = null;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		graphEdgesMap = new HashMap<String, HashMap<String, Integer>>();
	}

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String source = key.toString();

		for (Text value : values) {
			String valueStr = value.toString();
			String[] valueSplits = valueStr
					.split(FlightConstants.DELIMITER);
			String dest = valueSplits[0];
			int actualElapsedTime = Integer.parseInt(valueSplits[1]);

			if (graphEdgesMap.containsKey(source)) {
				HashMap<String, Integer> destMap = graphEdgesMap
						.get(source);

				if (destMap.containsKey(dest)) {
					int oldElapsedTime = destMap.get(dest);
					if (oldElapsedTime > actualElapsedTime) {
						destMap.put(dest, actualElapsedTime);
						graphEdgesMap.put(source, destMap);
					}
				} else {
					destMap.put(dest, actualElapsedTime);
					graphEdgesMap.put(source, destMap);
				}
			} else {
				HashMap<String, Integer> destMap = new HashMap<String, Integer>();
				destMap.put(dest, actualElapsedTime);
				graphEdgesMap.put(source, destMap);
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// int srcBitmapIndex = 0;

		// TODO
		// System.out.println("Num of sources: "
		// + graphEdgesMap.entrySet().size());

		for (String src : graphEdgesMap.keySet()) {
			HashMap<String, Integer> destinations = graphEdgesMap.get(src);

			NullWritable key = NullWritable.get();

			for (String destination : destinations.keySet()) {

				// TODO
				// Text value1 = new Text("R" + FlightConstants.DELIMITER +
				// src
				// + FlightConstants.DELIMITER + destination
				// + FlightConstants.DELIMITER
				// + destinations.get(destination));

				Text value1 = new Text("R" + FlightConstants.DELIMITER
						+ src + FlightConstants.DELIMITER + destination);

				// Emit
				context.write(key, value1);
			}

			// TODO
			// FMBitmask fmbValue = new FMBitmask();
			// Text value2 = new Text("BC" + FlightConstants.DELIMITER + src
			// + FlightConstants.DELIMITER + fmbValue.toString());

			Text value2 = new Text("BC" + FlightConstants.DELIMITER + src);

			context.write(key, value2);

			// srcBitmapIndex++;
		}

		graphEdgesMap = null;

		super.cleanup(context);
	}
}
