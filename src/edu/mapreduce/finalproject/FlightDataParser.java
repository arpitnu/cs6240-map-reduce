package edu.mapreduce.finalproject;

import java.io.IOException;
import java.io.StringReader;

import au.com.bytecode.opencsv.CSVReader;

/**
 * FlightDataParser Singleton Class
 * 
 * @author arpitm
 * 
 */
public class FlightDataParser {
	/**
	 * The instance of FlightDataParser class
	 */
	private static FlightDataParser instance = null;

	/**
	 * The CSV Reader object
	 */
	private static CSVReader csvReader;

	/**
	 * The string reader object
	 */
	private static StringReader strReader;

	/**
	 * Constructor
	 */
	private FlightDataParser() {
		// A private Constructor prevents any other class from instantiating.
	}

	/**
	 * getInstance
	 * 
	 * @return object FlightDataParser
	 */
	public static FlightDataParser getInstance() {
		if (instance == null) {
			instance = new FlightDataParser();
		}

		return instance;
	}

	/**
	 * getFlightData: Function parses the CSV flight data and returns a
	 * FlightData object.
	 * 
	 * @param String
	 *            line
	 * 
	 * @return object FlightData
	 * @throws IOException
	 */
	public FlightData getFlightData(String line) throws IOException {
		FlightData fData = null;

		strReader = new StringReader(line);
		csvReader = new CSVReader(strReader);

		String[] values = csvReader.readNext();

		// Extract fields
		int flightYear = Integer
				.parseInt(values[FlightConstants.INDEX_FLIGHT_YEAR].trim());
		int flightMonth = Integer
				.parseInt(values[FlightConstants.INDEX_FLIGHT_MONTH].trim());
		String flightDate = values[FlightConstants.INDEX_FLIGHT_DATE].trim();
		String origin = values[FlightConstants.INDEX_ORIGIN].trim();
		String dest = values[FlightConstants.INDEX_DESTINATION].trim();
		boolean isCancelled = (values[FlightConstants.INDEX_CANCELLED].trim()
				.equals("0.00")) ? false : true;
		String actualElapsedTimeStr = values[FlightConstants.INDEX_ACTUAL_ELAPSED_TIME]
				.trim();

		// Data sanity check
		if (isNullString(flightDate) || isNullString(origin)
				|| isNullString(dest) || isNullString(actualElapsedTimeStr)) {
			fData = null;
		} else {
			fData = new FlightData();
			Float actualElapsedTimeFl = Float.parseFloat(actualElapsedTimeStr);
			int actualElapsedTime = actualElapsedTimeFl.intValue();

			fData.setFlightYear(flightYear);
			fData.setFlightMonth(flightMonth);
			fData.setFlightDate(flightDate);
			fData.setOrigin(origin);
			fData.setDestination(dest);
			fData.setCancelled(isCancelled);
			fData.setActualElapsedTime(actualElapsedTime);
		}

		return fData;
	}

	/**
	 * Helper function to check is a string is null.
	 * 
	 * @param str
	 * 
	 * @return boolean
	 */
	private boolean isNullString(String str) {
		if ((str == null) || ("".equals(str)) || (str.length() == 0)) {
			return true;
		} else {
			return false;
		}
	}

	/*
	 * Get & Set methods
	 */
	public CSVReader getCsvReader() {
		return csvReader;
	}

	public void setCsvReader(CSVReader csvReader) {
		FlightDataParser.csvReader = csvReader;
	}

	public StringReader getStrReader() {
		return strReader;
	}

	public void setStrReader(StringReader strReader) {
		FlightDataParser.strReader = strReader;
	}
}
