package edu.mapred.assign4.hbase;

/**
 * Contains constants used by HPopulate & HCompute classes.
 * 
 * @author arpitm
 *
 */
public class HConstants {
	/**
	 * The table name
	 */
	public static final String FLIGHT_DATA_TABLE_NAME = "FlightData";
	
	/**
	 * The data column descriptor
	 */
	public static final String DATA_COLUMNFAMILY = "data";

	/**
	 * The delimiter used in table data
	 */
	public static final String DELIMITER = ":";
	
	/**
	 * The Data qualifier
	 */
	public static final String DATA_QUALIFIER = "flightdata";
	
	/**
	 * The year
	 */
	public static final int YEAR = 2008;
}
