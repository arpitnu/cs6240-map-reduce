package edu.mapred.assign4;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
 *         AverageFlightDelay Class
 * 
 */
public class AverageFlightDelay {

	public static class FlightDataMapper extends
			Mapper<Object, Text, Text, Text> {
		// FlightDataParser object
		private FlightDataParser dataParser;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);

			setParser(FlightDataParser.getInstance());
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// Get Flight Data
			FlightData fData = dataParser.getFlightData(value.toString());

			// Verify if flight is valid
			if (FlightUtils.isValidFlight(fData)) {
				// Define the key
				// FlightDataMapperKey outKey = null;
				Text outKey = null;

				// Define Value
				Text outValue = null;

				if (FlightUtils.isFirstFlight(fData)) {
					outKey = createKey(fData.getDestination(),
							fData.getFlightDate());

					outValue = createValue("F", fData.getArrTime(),
							fData.getArrDelay());
				} else if (FlightUtils.isSecondFlight(fData)) {
					outKey = createKey(fData.getOrigin(), fData.getFlightDate());

					outValue = createValue("S", fData.getDepTime(),
							fData.getArrDelay());
				}

				// TODO For testing
				// System.out.println(outKey + " ---> " + outValue);

				// Emit
				if ((outKey != null) && (outValue != null)) {
					context.write(outKey, outValue);
				}
			}
		}

		/**
		 * Function returns the out value for the mapper
		 * 
		 * @param f1orf2
		 * @param arrOrDepTime
		 * @param arrDelay
		 * 
		 * @return Text
		 */
		private Text createValue(String f1orf2, String arrOrDepTime,
				String arrDelay) {
			Text returnValue = null;

			if (!isNullString(arrOrDepTime) && !isNullString(arrDelay)) {
				returnValue = new Text(f1orf2 + FlightConstants.DELIMITER
						+ arrOrDepTime.trim() + FlightConstants.DELIMITER
						+ arrDelay.trim());
			}

			return returnValue;
		}

		/**
		 * Function return an output value for the mapper
		 * 
		 * @param dest
		 * @param date
		 * @return
		 */
		private Text createKey(String dest, String date) {
			Text returnKey = null;
			if (!isNullString(dest) && !isNullString(date)) {
				returnKey = new Text(dest.trim() + date.trim());
			}
			return returnKey;
		}

		/**
		 * Function checks if a string is null
		 * 
		 * @param str
		 * @return boolean
		 */
		private boolean isNullString(String str) {
			if ((str == null) || (str.trim().length() == 0)) {
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
		 * Get & set methods
		 */
		public FlightDataParser getParser() {
			return dataParser;
		}

		public void setParser(FlightDataParser parser) {
			this.dataParser = parser;
		}
	}

	/**
	 * FlightDataPartitioner Class
	 * <p>
	 * Partitions the FlightDataMapperKey based on their HashCode. The
	 * getPartition function returns partition number between 0 and
	 * numPartitions.
	 * </p>
	 * 
	 * @author arpitm
	 * 
	 */
	public static class FlightDataPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			
			String[] keyParts = key.toString().split("-");
			int month = Integer.parseInt(keyParts[1]);
			return (month - 1);

//			return ((key.toString().hashCode() * 127) % numPartitions);
		}
	}

	/**
	 * FlightDataReducer Class
	 * <p>
	 * This reducer performs the join operation on the FlightDataMapperKey
	 * object and outputs the delay for the joint flights.
	 * </p>
	 * 
	 * @author arpitm
	 * 
	 */
	public static class FlightDataReducer extends
			Reducer<Text, Text, Text, Text> {
		// List to store all flights in first leg
		private ArrayList<Text> lstFirstFlights = null;

		// List to store all flights in the second leg
		private ArrayList<Text> lstSecondFlights = null;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			lstFirstFlights = new ArrayList<Text>();
			lstSecondFlights = new ArrayList<Text>();
		}

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException,
				IndexOutOfBoundsException {
			// Clear lists
			lstFirstFlights.clear();
			lstSecondFlights.clear();

			boolean isFirstFlight = false;
			boolean isSecondFlight = false;

			// Join
			for (Text value : values) {
				String[] valueParts = value.toString().split(
						FlightConstants.DELIMITER);

				// TODO For testing
				// System.out.println(value.toString());

				isFirstFlight = (valueParts[0].equalsIgnoreCase("F") ? true
						: false);
				isSecondFlight = (valueParts[0].equalsIgnoreCase("S") ? true
						: false);

				if (isFirstFlight) {
					String arrTime = valueParts[1];
					String arrDelay = valueParts[2];
					lstFirstFlights.add(new Text(arrTime
							+ FlightConstants.DELIMITER + arrDelay));
				} else if (isSecondFlight) {
					String depTime = valueParts[1];
					String arrDelay = valueParts[2];
					lstSecondFlights.add(new Text(depTime
							+ FlightConstants.DELIMITER + arrDelay));
				}
			}

			// Execute our join logic now that the lists are filled
			executeJoinLogic(context);
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			super.cleanup(context);

			lstFirstFlights = null;
			lstSecondFlights = null;
		}

		/**
		 * Executes the reducer-side join logic based on the condition
		 * list1.flight.arrTime < list2.flight.depTime
		 * 
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 * @throws NumberFormatException
		 */
		private void executeJoinLogic(Context context) throws IOException,
				InterruptedException, NumberFormatException {
			int firstFlightArrtime = 0;
			int secondFlightDepTime = 0;
			float firstFlightArrDelay = (float) 0.0;
			float secondFlightArrDelay = (float) 0.0;
			long totalDelay = (long) 0;

			if (!lstFirstFlights.isEmpty() && !lstSecondFlights.isEmpty()) {
				for (Text firstFlight : lstFirstFlights) {
					String[] firstFlightParts = firstFlight.toString().split(
							FlightConstants.DELIMITER);

					firstFlightArrtime = Integer.parseInt(firstFlightParts[0]);
					firstFlightArrDelay = Float.parseFloat(firstFlightParts[1]);

					for (Text secondFlight : lstSecondFlights) {

						String[] secondFlightParts = secondFlight.toString()
								.split(FlightConstants.DELIMITER);

						secondFlightDepTime = Integer
								.parseInt(secondFlightParts[0]);
						secondFlightArrDelay = Float
								.parseFloat(secondFlightParts[1]);

						if ((secondFlightDepTime - firstFlightArrtime) > 0) {

							totalDelay = (long) firstFlightArrDelay
									+ (long) secondFlightArrDelay;

							// Increment Counters
							context.getCounter(
									FlightConstants.AverageFlightDelayCounters.DELAY_SUM)
									.increment(totalDelay);
							context.getCounter(
									FlightConstants.AverageFlightDelayCounters.FREQUENCY)
									.increment(1);
						}
					}
				}
			}
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
					.println("Usage: AverageFlightDelay <data-file-path> <output-path>");
			System.exit(1);
		}

		// Job: Average Flight Delay Calculation.
		Job job = new Job(conf, "Average Flight Delay Calculation.");
		// job.getConfiguration().set("join.type", joinType);
		job.setJarByClass(AverageFlightDelay.class);
		job.setMapperClass(FlightDataMapper.class);
		job.setPartitionerClass(FlightDataPartitioner.class);
		job.setReducerClass(FlightDataReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(FlightConstants.NUM_REDUCE_TASKS);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		job.waitForCompletion(true);

		// Calculate average flight delay once the job is successful
		if (job.isSuccessful()) {
			long delaySum = job
					.getCounters()
					.findCounter(
							FlightConstants.AverageFlightDelayCounters.DELAY_SUM)
					.getValue();
			long freq = job
					.getCounters()
					.findCounter(
							FlightConstants.AverageFlightDelayCounters.FREQUENCY)
					.getValue();

			float avgDelay = ((float) delaySum / (float) freq);

			System.out.println("Average flight delay from ORD -> JFK is: "
					+ avgDelay);
			System.exit(0);
		} else {
			System.out.println("Job " + job.getJobName() + " Failed!");
			System.exit(1);
		}
	}
}
