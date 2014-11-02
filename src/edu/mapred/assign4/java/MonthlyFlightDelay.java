package edu.mapred.assign4.java;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

/**
 * @author arpitm
 * 
 *         AverageFlightDelay Class
 * 
 */
public class MonthlyFlightDelay {

	/**
	 * Mapper class for flight data
	 * 
	 * @author arpitm
	 * 
	 */
	public static class FlightDataMapper extends
			Mapper<Object, Text, FlightDataMapperKey, Text> {
		// parser
		private FlightDataParser dataParser;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);

			setParser(FlightDataParser.getInstance());
		}

		/**
		 * function parses the input data and outputs (k,v) pair. k is of type
		 * FlightDataMapperKey and value is of type Text
		 */
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// Get Flight Data
			FlightData fData = dataParser.getFlightData(value.toString());

			// Define Mapper key and value
			FlightDataMapperKey outKey = null;
			Text outValue = null;

			if (FlightUtils.isValidFlight(fData)) {
				outKey = createKey(fData.getAirlineId().trim(),
						fData.getFlightMonth());
				outValue = createValue(fData.getArrDelay().trim());

				// Emit
				if ((outKey != null) && (outValue != null)) {
					// TODO For testing
					// System.out.println(outKey.toString() + " ---> " +
					// outValue);

					context.write(outKey, outValue);
				}
			}
		}

		/**
		 * Function returns an value for the map function
		 * 
		 * @param arrDelay
		 * 
		 * @return Text returnValue
		 */
		private Text createValue(String arrDelay) {
			Text returnValue = null;

			if (!isNullString(arrDelay)) {
				returnValue = new Text(arrDelay.trim());
			}

			return returnValue;
		}

		/**
		 * returns a mapper key
		 * 
		 * @param airlineId
		 * @param month
		 * 
		 * @return FlightDataMapperKey key
		 */
		private FlightDataMapperKey createKey(String airlineId, int month) {
			FlightDataMapperKey key = null;

			if (!isNullString(airlineId) && (month >= 1) && (month <= 12)) {
				key = new FlightDataMapperKey(airlineId, month);
			}
			return key;
		}

		/**
		 * Helper function to check is a string is null.
		 * 
		 * @param str
		 * 
		 * @return boolean
		 */
		private boolean isNullString(String str) {
			if ((str == null) || (str.length() == 0)) {
				return true;
			} else {
				return false;
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException, NullPointerException {
			// Close the parser's string reader
			dataParser.getStrReader().close();

			// Close the parser's CSV reader
			dataParser.getCsvReader().close();

			super.cleanup(context);
		}

		/*
		 * Get & set methods for data parser.
		 */
		public FlightDataParser getParser() {
			return dataParser;
		}

		public void setParser(FlightDataParser parser) {
			this.dataParser = parser;
		}
	}

	/**
	 * Partitions the FlightDataMapperKey based on the HashCode of airlineId.
	 * The getPartition function returns partition number between 0 and
	 * numPartitions.
	 * 
	 * @author arpitm
	 * 
	 */
	public static class FlightDataPartitioner extends
			Partitioner<FlightDataMapperKey, Text> {

		@Override
		public int getPartition(FlightDataMapperKey key, Text value,
				int numPartitions) {
			int partitionNum = (Math.abs(key.getAirlineId().hashCode()) % numPartitions);

			return partitionNum;
		}
	}

	/**
	 * Groups input keys to the reducer based on the airlineId of the keys.
	 * 
	 * @author arpitm
	 * 
	 */
	public static class FlightDataGroupComparator extends WritableComparator {

		protected FlightDataGroupComparator() {
			super(FlightDataMapperKey.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			FlightDataMapperKey key1 = (FlightDataMapperKey) a;
			FlightDataMapperKey key2 = (FlightDataMapperKey) b;

			return (key1.getAirlineId().compareTo(key2.getAirlineId()));
		}
	}

	/**
	 * Sort comparator class sorts the reducer input keys based on the airlineId
	 * and the month.
	 * 
	 * @author arpitm
	 * 
	 */
	public static class FlightDataSortComparator extends WritableComparator {

		protected FlightDataSortComparator() {
			super(FlightDataMapperKey.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			FlightDataMapperKey key1 = (FlightDataMapperKey) a;
			FlightDataMapperKey key2 = (FlightDataMapperKey) b;

			int cmpResult = key1.getAirlineId().compareTo(key2.getAirlineId());

			if (cmpResult == 0) {
				int m1 = key1.getMonth();
				int m2 = key2.getMonth();

				cmpResult = FlightDataMapperKey.compareMonths(m1, m2);
			}

			return cmpResult;
		}
	}

	/**
	 * This reducer computes the average delay for each month for the input
	 * FlightDataMapperKey.
	 * 
	 * @author arpitm
	 * 
	 */
	public static class MonthlyFlightDataReducer extends
			Reducer<FlightDataMapperKey, Text, Text, Text> {

		public void reduce(FlightDataMapperKey key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			// variable to hold total delay for each month
			double totalDelay = 0.0;

			// variable to hold count value for each month
			int totalCount = 0;

			// integer array to hold monthly average delay.
			int[] monthAvgDelay = new int[12];

			// previous month index
			int prevMonth = 1;

			for (Text value : values) {
				int month = key.getMonth();

				if (prevMonth != month) {
					monthAvgDelay[prevMonth - 1] = (int) Math.ceil(totalDelay
							/ totalCount);

					// Reset values
					totalDelay = 0.0;
					totalCount = 0;
					prevMonth = month;
				}

				totalCount = totalCount + 1;

				float delayMinutes = Float.parseFloat(value.toString());
				totalDelay += delayMinutes;
			}

			// Average delay for last month
			monthAvgDelay[prevMonth - 1] = (int) Math.ceil(totalDelay
					/ totalCount);

			// String builder to hold the value string
			StringBuilder valueSb = new StringBuilder();

			// Loop through values to build reducer value string
			for (int i = 0; i < monthAvgDelay.length; i++) {
				if (i > 0) {
					valueSb.append(", ");
				}

				valueSb.append("(").append(i + 1).append(",")
						.append(monthAvgDelay[i]).append(")");
			}

			// Emit
			context.write(new Text(key.getAirlineId()),
					new Text(valueSb.toString()));
		}
	}

	/**
	 * main: Driver function
	 * 
	 * @param args
	 * 
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		// Arguments length check
		if (otherArgs.length != 2) {
			System.err
					.println("Usage: MonthlyFlightDelay <input-file-path> <output-dir-path>");
			System.exit(2);
		}

		// Job: Monthly Average Flight Delay Calculation.
		Job job = new Job(conf,
				"Airline-wise Monthly Average Flight Delay Calculation.");
		job.setJarByClass(MonthlyFlightDelay.class);
		job.setMapperClass(FlightDataMapper.class);
		job.setPartitionerClass(FlightDataPartitioner.class);
		job.setGroupingComparatorClass(FlightDataGroupComparator.class);
		job.setSortComparatorClass(FlightDataSortComparator.class);
		job.setReducerClass(MonthlyFlightDataReducer.class);

		job.setMapOutputKeyClass(FlightDataMapperKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// job.setNumReduceTasks(FlightConstants.NUM_REDUCE_TASKS);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
