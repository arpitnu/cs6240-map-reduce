/**
 * Program calculates the diameter of the Flight Data graph, i.e, the longest shortest path.
 */
package edu.mapred.finalproject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.mapred.assign4.java.FlightDataMapperKey;

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

				Text k = null;
				Text v = null;

				if (inputType.equals("BC")) {
					// Check if iteration 0
					String bitmaskCommand = context.getConfiguration().get(
							"bitmaskCommand");
					if (bitmaskCommand.equals("BC")) {
						src = lineParts[1];
						k = new Text(src);
						v = new Text(inputType + FlightConstants.DELIMITER);
					}
				} else if (inputType.equals("B")) {
					src = lineParts[1];
					String bitMaskStr = lineParts[2];
					k = new Text(src);
					v = new Text(inputType + FlightConstants.DELIMITER
							+ bitMaskStr);
				} else if (inputType.equals("R")) {
					src = lineParts[1];
					dest = lineParts[2];
					k = new Text(dest);
					v = new Text(inputType + FlightConstants.DELIMITER + src);
				} else {
					System.err
							.println("HADI Stage 1 Mapper: Input Type Unrecognized!");
					System.exit(-2);
				}

				// Emit
				context.write(k, v);
			}
		}
	}

	public static class HADIStage1Partitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
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
		ArrayList<String> listSources = null;
		FMBitmask fmb = null;
		String destNode = null;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			destNode = new String();
			listSources = new ArrayList<String>();
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			destNode = key.toString();
			// NullWritable k = NullWritable.get();
			// Text v = null;

			for (Text valueText : values) {
				String[] valueSplits = valueText.toString().split(
						FlightConstants.DELIMITER);
				String inputType = valueSplits[0];
				if (inputType.equals("BC")) {
					fmb = new FMBitmask();
				} else if (inputType.equals("B")) {
					String fmbStr = valueSplits[1];
					fmb = new FMBitmask(fmbStr);
				} else if (inputType.equals("R")) {
					String node = valueSplits[1];
					if (!listSources.contains(node)) {
						listSources.add(node);
					}
				} else {
					System.err
							.println("HADI Stage 1 Reducer: Input Type Unrecognized!");
					System.exit(-2);
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			NullWritable key = NullWritable.get();
			Text value = null;

			if (!listSources.isEmpty()) {
				for (String source : listSources) {
					value = new Text(source + FlightConstants.DELIMITER
							+ fmb.toString());
					context.write(key, value);
				}
			}

			if (!destNode.isEmpty() && !listSources.contains(destNode)) {
				value = new Text(destNode + FlightConstants.DELIMITER
						+ fmb.toString());
				context.write(key, value);
			}

			listSources = null;
			fmb = null;
			destNode = null;
			super.cleanup(context);
		}
	}

	public static class HADIStage2Mapper extends
			Mapper<Object, Text, Text, Text> {
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
		}
	}
	
	public static class HADIStage2Partitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			return 0;
		}
		
	}

	public static class HADIStage2Reducer extends
			Reducer<Text, Text, NullWritable, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
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
		if (otherArgs.length != 2) {
			System.err
					.println("Usage: GraphDiameter <directory-path> <MM-DD-YYYY>");
			System.exit(2);
		}

		// Input and output paths
		String dirPath = otherArgs[0];
		String currentDate = otherArgs[1];

		// Begin from start date
		// int currentDay = Integer.parseInt(FlightConstants.START_DAY);
		// int currentMonth = Integer.parseInt(FlightConstants.START_MONTH);
		// int currentYear = Integer.parseInt(FlightConstants.END_YEAR);
		// int endDay = Integer.parseInt(FlightConstants.END_DAY);
		// int endMonth = Integer.parseInt(FlightConstants.END_MONTH);
		// int endYear = Integer.parseInt(FlightConstants.END_YEAR);

		// while ((currentDay <= endDay) && (currentMonth <= endMonth)
		// && (currentYear <= endYear)) {
		// String currentDate = currentMonth + "-" + currentDay + "-"
		// + currentYear;

		// TODO
		// System.out.println("Current Date = " + currentDate);

		// Configuration conf = new Configuration();
		// TODO Required?
		conf.set("currentDate", currentDate);

		// Job 1: Graph generation
		String job1Name = "Flight Data Graph Generation For Date "
				+ currentDate;
		// TODO
		System.out.println("Starting Job: " + job1Name);

		Job job1 = new Job(conf, job1Name);
		job1.setJarByClass(GraphDiameter.class);
		job1.setMapperClass(GraphMapper.class);
		job1.setReducerClass(GraphReducer.class);
		// job1.setPartitionerClass(GraphPartitioner.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(NullWritable.class);
		job1.setOutputValueClass(Text.class);
		Path job1InputPath = new Path(dirPath + "/input/Jan-2014.csv");
		Path job1OutputPath = new Path(dirPath + "/output/" + currentDate
				+ "/job1/");

		// TODO
		System.out.println("Deleting old output directory " + dirPath
				+ "/output/" + currentDate + "/job1/");
		FileSystem.getLocal(conf).delete(job1OutputPath, true);

		FileInputFormat.addInputPath(job1, job1InputPath);
		FileOutputFormat.setOutputPath(job1, job1OutputPath);
		int job1CompletionStatus = (job1.waitForCompletion(true) ? 0 : 1);

		if (job1CompletionStatus == 0) {
			// TODO
			System.out.println("Job " + job1Name + " completed successfully!");
			// System.exit(0);

			int iteration = 0;
			boolean hasConverged = false;

			// HADI Stage 1: Invert Edge, match bitmasks to node id.
			Configuration hadiStage1Conf = new Configuration();

			if (0 == iteration) {
				hadiStage1Conf.set("bitmaskCommand", "BC");
			} else {
				hadiStage1Conf.set("bitmaskCommand", "B");
			}

			String hadiStage1JobName = "HADI Stage 1";

			// TODO
			System.out.println("Starting Job: " + job1Name);

			Job hadiStage1Job = new Job(hadiStage1Conf, hadiStage1JobName);
			hadiStage1Job.setJarByClass(GraphDiameter.class);
			hadiStage1Job.setMapperClass(HADIStage1Mapper.class);
			hadiStage1Job.setReducerClass(HADIStage1Reducer.class);
			hadiStage1Job.setPartitionerClass(HADIStage1Partitioner.class);
			hadiStage1Job
					.setGroupingComparatorClass(HADIStage1GroupComparator.class);
			hadiStage1Job.setMapOutputKeyClass(Text.class);
			hadiStage1Job.setOutputKeyClass(NullWritable.class);
			hadiStage1Job.setOutputValueClass(Text.class);
			Path hadiStage1OutputPath = new Path(dirPath + "/output/"
					+ currentDate + "/iteration-" + iteration + "/hadi-stage1/");
			FileOutputFormat.setOutputPath(hadiStage1Job, hadiStage1OutputPath);

			// TODO
			System.out.println("Deleting old output directory " + dirPath
					+ "/output/" + currentDate + "/iteration-" + iteration
					+ "/hadi-stage1/");
			FileSystem.getLocal(conf).delete(hadiStage1OutputPath, true);

			if (0 == iteration) {
				Path hadiStage1InputPath = job1OutputPath;
				FileInputFormat
						.addInputPath(hadiStage1Job, hadiStage1InputPath);
			} else {
				// TODO
				Path hadiStage1InputPath1 = job1OutputPath;
				Path hadiStage1InputPath2 = new Path(dirPath + "/output/"
						+ currentDate + "/iteration-" + (iteration - 1)
						+ "/hadi-stage2/");
				FileInputFormat.addInputPath(hadiStage1Job,
						hadiStage1InputPath1);
				FileInputFormat.addInputPath(hadiStage1Job,
						hadiStage1InputPath2);
			}

			int hadiS1CompletionStatus = (hadiStage1Job.waitForCompletion(true) ? 0
					: 1);
			if (hadiS1CompletionStatus == 0) {
				// TODO
				System.out.println("Job: " + hadiStage1JobName
						+ " completed successfully!");
				// System.exit(0);

				// HADI Stage 2: Merge bitmasks for each node.
				Configuration hadiStage2Conf = new Configuration();
				String hadiStage2JobName = "HADI Stage 2";

				// TODO
				System.out.println("Starting Job: " + job1Name);

				Job hadiStage2Job = new Job(hadiStage2Conf, hadiStage2JobName);
				hadiStage2Job.setJarByClass(GraphDiameter.class);
			} else {
				System.err
						.println("JOB STATUS MESSAGE: HADI Stage 1 Job for date "
								+ currentDate + " failed!");
				System.exit(-2);
			}

		} else {
			System.err.println("JOB STATUS MESSAGE: Job 1 for date "
					+ currentDate + " failed!");
			System.exit(-2);
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

	// }

}
