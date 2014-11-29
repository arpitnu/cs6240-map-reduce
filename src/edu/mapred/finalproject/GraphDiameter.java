/**
 * 
 */
package edu.mapred.finalproject;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @author arpitm
 * 
 */
public class GraphDiameter {
	public static class GraphMapper extends Mapper<Object, Text, Text, Text> {
		// Hashmap
		HashMap<String, HashMap<String, Integer>> edgesMap = null;

		// Flight data parser
		FlightDataParser fDataParser = null;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);

			edgesMap = new HashMap<String, HashMap<String, Integer>>();
			fDataParser = FlightDataParser.getInstance();
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// Get flight data object from input
			FlightData fData = fDataParser.getFlightData(value.toString());

			if (fData != null) {

				String source = fData.getOrigin();
				String dest = fData.getDestination();
				String flightDate = fData.getFlightDate();
				boolean iscancelled = fData.isCancelled();
				int actualElapsedTime = fData.getActualElapsedTime();

				if (flightDate.equals(FlightConstants.GRAPH_DATE)
						&& (iscancelled == false)) {
					if (edgesMap.containsKey(source)) {
						HashMap<String, Integer> destMap = edgesMap.get(source);

						if (destMap.containsKey(dest)) {
							int oldElapsedTime = destMap.get(dest);

							if (oldElapsedTime > actualElapsedTime) {
								destMap.put(dest, actualElapsedTime);
								edgesMap.put(source, destMap);
							}

						} else {
							destMap.put(dest, actualElapsedTime);
							edgesMap.put(source, destMap);
						}
					} else {
						HashMap<String, Integer> destMap = new HashMap<String, Integer>();
						destMap.put(dest, actualElapsedTime);
						edgesMap.put(source, destMap);
					}
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (String src : edgesMap.keySet()) {
				HashMap<String, Integer> destinations = edgesMap.get(src);

				for (String destination : destinations.keySet()) {
					Text key = new Text(src);
					Text value = new Text(destination
							+ FlightConstants.DELIMITER
							+ destinations.get(destination));

					// Emit
					context.write(key, value);
				}
			}

			edgesMap = null;

			super.cleanup(context);
		}
	}

	public static class GraphPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			String source = key.toString();

			return source.hashCode() % numPartitions;
		}
	}

	public static class GraphReducer extends
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
			for (String src : graphEdgesMap.keySet()) {
				HashMap<String, Integer> destinations = graphEdgesMap.get(src);

				for (String destination : destinations.keySet()) {
					NullWritable key = NullWritable.get();
					// TODO
					// Text value = new Text(src + FlightConstants.DELIMITER
					// + destination + FlightConstants.DELIMITER
					// + destinations.get(destination));
					Text value = new Text(src + FlightConstants.DELIMITER
							+ destination);

					// Emit
					context.write(key, value);
				}
			}

			graphEdgesMap = null;

			super.cleanup(context);
		}
	}

	public static class HADIStage1Mapper extends Mapper<Text, Text, Text, Text> {

	}

	public static class HADIStage1Partitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			String src = key.toString();
			return src.hashCode() % numPartitions;
		}

	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		// Input sanity check
		if (otherArgs.length != 1) {
			System.err.println("Usage: GraphDiameter <directory-path>");
			System.exit(2);
		}

		// Input and output paths
		String dirPath = otherArgs[0];

		int currentDay = FlightConstants.START_DAY;
		int currentMonth = FlightConstants.START_MONTH;
		int currentYear = FlightConstants.END_YEAR;

		while ((currentDay <= FlightConstants.END_DAY)
				&& (currentMonth <= FlightConstants.END_MONTH)
				&& (currentYear <= FlightConstants.END_YEAR)) {
			String currentDate = currentMonth + "-" + currentDay + "-"
					+ currentYear;

			// TODO
			System.out.println("Current Date = " + currentDate);

			// Configuration conf = new Configuration();
			conf.set("currentDate", currentDate);

			// Job 1: Graph generation
			String job1Name = "Flight Data Graph Generation For Date "
					+ currentDate;
			// TODO
			System.out.println("Starting Job 1: " + job1Name);

			Job job1 = new Job(conf, job1Name);
			job1.setJarByClass(GraphDiameter.class);
			// job1.setPartitionerClass(GraphPartitioner.class);

			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(Text.class);
			job1.setOutputKeyClass(NullWritable.class);
			job1.setOutputValueClass(Text.class);
			Path job1InputPath = new Path(dirPath + "/input/data.csv");
			Path job1OutputPath = new Path(dirPath + "/output/" + currentDate
					+ "/job1/");
			FileInputFormat.addInputPath(job1, job1InputPath);
			FileOutputFormat.setOutputPath(job1, job1OutputPath);
			int job1CompletionStatus = (job1.waitForCompletion(true) ? 0 : 1);

			if (job1CompletionStatus == 0) {
				// TODO
				System.exit(0);
			} else {
				System.err.println("JOB STATUS MESSAGE: Job 1 for date "
						+ currentDate + " failed!");
			}
		}

		// Initialize counters
		// job.getCounters()
		// .findCounter(
		// FlightConstants.FlightDataGraphCounters.ITERATION_COUNTER)
		// .setValue(0);
		// job.getCounters()
		// .findCounter(
		// FlightConstants.FlightDataGraphCounters.ITERATION_STOP_COUNTER)
		// .setValue(0);

	}

}
