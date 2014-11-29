/**
 * This map-reduce job constructs the graph 
 */
package edu.mapreduce.finalproject;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
public class GenerateGraph {
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
						// HashMap<String, HashMap<String, Integer>> sourceMap =
						// new
						// HashMap<String, HashMap<String, Integer>>();
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

	public static class GraphReducer extends Reducer<Text, Text, NullWritable, Text> {
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
					Text value = new Text(src + FlightConstants.DELIMITER
							+ destination + FlightConstants.DELIMITER
							+ destinations.get(destination));

					// Emit
					context.write(key, value);
				}
			}

			graphEdgesMap = null;

			super.cleanup(context);
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

		// Arguments length check
		if (otherArgs.length != 2) {
			System.err
					.println("Usage: GenerateGraph <input-file-path> <output-dir-path>");
			System.exit(2);
		}

		Job job = new Job(conf, "Generate flight data graph");
		job.setJarByClass(GenerateGraph.class);
		job.setMapperClass(GraphMapper.class);
		job.setReducerClass(GraphReducer.class);
		// job.setPartitionerClass(GraphPartitioner.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		// TODO
		System.out.println("Deleting old output directory");

		FileSystem.getLocal(conf).delete(new Path(otherArgs[1]), true);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
