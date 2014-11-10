package edu.mapred.assign4.java;

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
		FlightData fData = new FlightData();

		strReader = new StringReader(line);
		csvReader = new CSVReader(strReader);

		String[] values = csvReader.readNext();

		fData.setFlightYear(Integer
				.parseInt(values[FlightConstants.INDEX_FLIGHT_YEAR].trim()));
		fData.setFlightMonth(Integer
				.parseInt(values[FlightConstants.INDEX_FLIGHT_MONTH].trim()));
		fData.setArrDelay(values[FlightConstants.INDEX_ARR_DELAY_MINUTES]
				.trim());
		fData.setCancelled((values[FlightConstants.INDEX_CANCELLED].trim()
				.equals("0.00")) ? false : true);
		fData.setDiverted((values[FlightConstants.INDEX_DIVERTED].trim()
				.equals("0.00")) ? false : true);
		fData.setAirlineId(values[FlightConstants.INDEX_AIRLINE_ID].trim());

		return fData;
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
