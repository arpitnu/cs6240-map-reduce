package edu.mapred.assign4;

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
	public static final int NUM_REDUCE_TASKS = 12;

	/**
	 * From year
	 */
	 public static final int FROM_YEAR = 2007;

	/**
	 * To year
	 */
	public static final int TO_YEAR = 2008;

	/**
	 * From month
	 */
	 public static final int FROM_MONTH = 6;

	/**
	 * To month
	 */
	public static final int TO_MONTH = 5;

	/**
	 * Origin airport code
	 */
	public static final String ORIGIN_CODE = "ORD";

	/**
	 * Destination airport code
	 */
	 public static final String DESTINATION_CODE = "JFK";

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
	public static final int INDEX_ORIGIN = 11;
	public static final int INDEX_DESTINATION = 17;
	public static final int INDEX_DEP_TIME = 24;
	public static final int INDEX_ARR_TIME = 35;
	public static final int INDEX_ARR_DELAY_MINUTES = 37;
	public static final int INDEX_CANCELLED = 41;
	public static final int INDEX_DIVERTED = 43;
	
	/**
	 * Enum for global counters to calculate average flight delay
	 */
	public enum AverageFlightDelayCounters {
		DELAY_SUM,
		FREQUENCY
	};
}
