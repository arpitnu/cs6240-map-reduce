package edu.mapred.assign4.java;

/**
 * @author arpitm
 * 
 *         FlightConstants
 * 
 *         Class contains constants for flight data processing
 * 
 */
public class FlightConstants {
	/**
	 * Number of reduce tasks
	 */
	public static final int NUM_REDUCE_TASKS = 20;

	/**
	 * The year
	 */
	public static final int YEAR = 2008;

	/**
	 * The delimiter used in Mappers
	 */
	public static final String DELIMITER = ":";

	/**
	 * Fixed fields in CSV flight data
	 */
	public static final int INDEX_FLIGHT_YEAR = 0;
	public static final int INDEX_FLIGHT_MONTH = 2;
	public static final int INDEX_FLIGHT_DATE = 5;
	public static final int INDEX_AIRLINE_ID = 7;
	public static final int INDEX_ORIGIN = 11;
	public static final int INDEX_DESTINATION = 17;
	public static final int INDEX_DEP_TIME = 24;
	public static final int INDEX_ARR_TIME = 35;
	public static final int INDEX_ARR_DELAY_MINUTES = 37;
	public static final int INDEX_CANCELLED = 41;
	public static final int INDEX_DIVERTED = 43;
}
