package edu.mapred.assign4.java;

/**
 * @author arpitm
 * 
 */
public class FlightUtils {

	/**
	 * Function checks if the flight is of interest.
	 * 
	 * @param fData
	 *            FlightData object
	 * 
	 * @return boolean isValid
	 */
	public static boolean isValidFlight(FlightData fData) {
		boolean isValid = false;

		isValid = (!fData.isCancelled() && isYearValid(fData.getFlightYear()));

		return isValid;
	}

	private static boolean isYearValid(int flightYear) {
		boolean yearValid = false;

		if (flightYear == FlightConstants.YEAR) {
			yearValid = true;
		}

		return yearValid;
	}
}
