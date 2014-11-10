package edu.mapred.assign4.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HCompute {

	public static class HComputeMapper extends Mapper<Object, Text, Text, Text> {
		/**
		 * The client HBase table
		 */
		HTable cliTable = null;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);

			// Initialize HBase table.
			cliTable = new HTable(context.getConfiguration(),
					HConstants.FLIGHT_DATA_TABLE_NAME);
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// integer array to hold monthly average delay.
			int[] monthTotalDelay = new int[12];

			// integer array for count of the flight for each month.
			int[] monthCount = new int[12];

			// Get the scan string
			Scan inputScan = getFlightDataScan(value.toString());

			ResultScanner resultScanner = cliTable.getScanner(inputScan);

			// Loop through the scan result
			for (Result result : resultScanner) {
				byte[] valBytes = result.getValue(
						Bytes.toBytes(HConstants.DATA_COLUMNFAMILY),
						Bytes.toBytes(HConstants.DATA_QUALIFIER));
				String val = Bytes.toString(valBytes);

				String[] valParts = val.split(HConstants.DELIMITER);
				int month = Integer.parseInt(valParts[0]);
				int delayMinutes = Integer.parseInt(valParts[1]);

				// Increment delay and count
				monthTotalDelay[month - 1] += delayMinutes;
				monthCount[month - 1] += 1;
			}

			// Build output string
			StringBuilder sBuilder = new StringBuilder();

			// Loop through values to build reducer value string
			for (int i = 0; i < monthTotalDelay.length; i++) {
				if (i > 0) {
					sBuilder.append(", ");
				}

				int monthAvgDelay = (int) Math
						.ceil(((double) monthTotalDelay[i]) / monthCount[i]);

				sBuilder.append("(").append(i + 1).append(",")
						.append(monthAvgDelay).append(")");
			}

			// Emit
			context.write(value, new Text(sBuilder.toString()));

			// Close result Scanner
			if (resultScanner != null) {
				resultScanner.close();
			}
		}

		private Scan getFlightDataScan(String airlineId) {
			// Need to start scanning all keys with input
			// "airlineId + YEAR + Not cancelled + Not delayed"
			String startScanString = airlineId + HConstants.YEAR + "00";
			long startScanKey = Long.parseLong(startScanString);
			byte[] startScanRKey = Bytes.padTail(Bytes.toBytes(startScanKey),
					Bytes.SIZEOF_LONG);

			// Need to start scanning all keys with input
			// "airlineId + YEAR + cancelled + Not delayed"
			String stopScanString = airlineId + HConstants.YEAR + "01";
			long stopScanKey = Long.parseLong(stopScanString);
			byte[] stopScanRKey = Bytes.padTail(Bytes.toBytes(stopScanKey),
					Bytes.SIZEOF_LONG);

			// Scan table.
			Scan scan = new Scan(startScanRKey, stopScanRKey);
			scan.setCaching(500);
			scan.setCacheBlocks(false);

			return scan;
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// Close the HTable
			cliTable.close();

			super.cleanup(context);
		}

	}

	/**
	 * driver function
	 * 
	 * @param otherArgs
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] otherArgs) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		// Arguments length check
		if (otherArgs.length != 2) {
			System.err
					.println("Usage: HCompute <airline-id-file-path> <output-dir-path>");
			System.exit(2);
		}

		// Job: Airline-wise monthly average delay computation
		Job job = new Job(conf, "Airline-wise monthly avg delay calculation");
		job.setJarByClass(HCompute.class);
		job.setMapperClass(HComputeMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(0);

		// Read the airlines list from the txt files. 16 id's at a time
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileInputFormat.setMaxInputSplitSize(job, 16);
		
		// Set output path
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
