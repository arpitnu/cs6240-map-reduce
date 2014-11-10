/**
 * 
 */
package edu.mapred.assign4.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.mapred.assign4.java.FlightData;
import edu.mapred.assign4.java.FlightDataParser;

/**
 * @author arpitm
 * 
 */
public class HPopulate {
	/**
	 * Mapper inserts data in HBase Table
	 * 
	 * @author arpitm
	 * 
	 */
	public static class HPopulateMapper extends
			Mapper<Object, Text, ImmutableBytesWritable, Put> {
		/**
		 * The parser to get FlightData object.
		 */
		FlightDataParser fDataParser = FlightDataParser.getInstance();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			FlightData fData = fDataParser.getFlightData(value.toString());

			int flightMonth = fData.getFlightMonth();
			String flightDelayStr = fData.getArrDelay();

			// Check if data is not null
			if (!(isNullStr(flightDelayStr) && (flightMonth == 0))) {
				String[] flightDelayStrParts = flightDelayStr.split("\\.");

				// Create row key for the hbase table
				byte[] rKey = createRowKey(fData);

				// Create data for the 'data' column of the table
				String tableData = flightMonth + HConstants.DELIMITER
						+ flightDelayStrParts[0].trim();

				Put put = new Put(rKey);
				put.add(Bytes.toBytes(HConstants.DATA_COLUMNFAMILY),
						Bytes.toBytes(HConstants.DATA_QUALIFIER),
						Bytes.toBytes(tableData));

				context.write(new ImmutableBytesWritable(rKey), put);
			}
		}

		/**
		 * Program checks if the string is null or its length is 0
		 * 
		 * @param str
		 * @return
		 */
		private boolean isNullStr(String str) {
			if (("".equals(str)) || (str == null) || (str.length() == 0)) {
				return true;
			} else {
				return false;
			}
		}

		/**
		 * returns a byte[] row key for the HBase table. The key contains the
		 * 
		 * @param fData
		 * @return
		 */
		private byte[] createRowKey(FlightData fData) {
			String key = fData.getAirlineId() + fData.getFlightYear()
					+ (fData.isCancelled() ? "1" : "0")
					+ (fData.isDiverted() ? "1" : "0");

			// The byte[] table row key
			byte[] rKey = new byte[2 * Bytes.SIZEOF_LONG];

			Bytes.putBytes(rKey, 0, Bytes.toBytes(Long.parseLong(key)), 0,
					Bytes.SIZEOF_LONG);

			// Add timestamp to rowKey
			long timeStamp = System.nanoTime();
			Bytes.putLong(rKey, Bytes.SIZEOF_LONG, timeStamp);

			return rKey;
		}
	}

	/**
	 * driver function
	 * 
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = HBaseConfiguration.create();

		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		// Arguments length check
		if (otherArgs.length != 1) {
			System.err.println("Usage: HPopulate <input-file-path>");
			System.exit(2);
		}

		// Create table
		HBaseAdmin admin = new HBaseAdmin(conf);

		// Check if the table already exists
		if (!admin.tableExists(HConstants.FLIGHT_DATA_TABLE_NAME)) {
			HTableDescriptor htd = new HTableDescriptor(
					HConstants.FLIGHT_DATA_TABLE_NAME);
			HColumnDescriptor hcd = new HColumnDescriptor(
					HConstants.DATA_COLUMNFAMILY);
			htd.addFamily(hcd);
			admin.createTable(htd);
		} else {
			System.out.println("HTable " + HConstants.FLIGHT_DATA_TABLE_NAME
					+ " already exists!");
		}

		// TODO For testing
		System.out.println("HBase table " + HConstants.FLIGHT_DATA_TABLE_NAME
				+ "created.");

		// The job: Populate HBase table
		Job job = new Job(conf, "HBase table populate");
		job.setJarByClass(HPopulate.class);
		job.setMapperClass(HPopulateMapper.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Put.class);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE,
				HConstants.FLIGHT_DATA_TABLE_NAME);
		job.setNumReduceTasks(0);

		// FileInputFormat
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

		System.exit(job.waitForCompletion(true) ? 0 : 2);

		admin.close();
	}

}
