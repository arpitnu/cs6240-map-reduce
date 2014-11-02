/**
 * 
 */
package edu.mapred.assign4.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author arpitm
 *
 */
public class hpopulate {
	public static class FlightDataMapper extends Mapper<Object, Text, ImmutableBytesWritable, Put> {
		
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
