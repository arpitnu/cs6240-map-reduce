package edu.mapred.assign4;

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

		isValid = (isMonthAndYearValid(fData)
				&& isOriginOrDestinationValid(fData) && !(fData.isCancelled() && fData
				.isDiverted()));

		return isValid;
	}

	/**
	 * Function checks if the flight matches the criteria for the first flight.
	 * 
	 * @param fData
	 * @return isValid
	 */
	public static boolean isFirstFlight(FlightData fData) {
		boolean isValid = false;
		String origin = fData.getOrigin().trim();
		String destination = fData.getDestination().trim();

		isValid = (origin.equalsIgnoreCase(FlightConstants.ORIGIN_CODE) && !destination
				.equalsIgnoreCase(FlightConstants.DESTINATION_CODE));

		return isValid;
	}

	/**
	 * Function checks if the flight matches the criteria for the second flight.
	 * 
	 * @param fData
	 * @return isValid
	 */
	public static boolean isSecondFlight(FlightData fData) {
		boolean isValid = false;
		String origin = fData.getOrigin().trim();
		String destination = fData.getDestination().trim();

		isValid = (!origin.equalsIgnoreCase(FlightConstants.ORIGIN_CODE) && destination
				.equalsIgnoreCase(FlightConstants.DESTINATION_CODE));

		return isValid;
	}

	/**
	 * isOriginOrDestinationValid: Function checks if the flight's origin or
	 * destination is valid
	 * 
	 * @param fData
	 * 
	 * @return
	 */
	private static boolean isOriginOrDestinationValid(FlightData fData) {
		boolean isValid = false;
		
		isValid = (isFirstFlight(fData) || isSecondFlight(fData));

		return isValid;
	}

	/**
	 * Function checks if the month & the year of the flight is within range.
	 * 
	 * @param fData
	 * 
	 * @return boolean isValid
	 */
	private static boolean isMonthAndYearValid(FlightData fData) {
		boolean isValid = false;
		int year = fData.getFlightYear();
		int month = fData.getFlightMonth();

		// Checks
		if (year == FlightConstants.FROM_YEAR) {
			if ((month > FlightConstants.FROM_MONTH) && (month <= 12)) {
				isValid = true;
			}
		} else if (year == FlightConstants.TO_YEAR) {
			if ((month < FlightConstants.TO_MONTH) && (month >= 1)) {
				isValid = true;
			}
		} else if ((year > FlightConstants.FROM_YEAR)
				&& (year < FlightConstants.TO_YEAR)) {
			isValid = true;
		} else {
			isValid = false;
		}

		return isValid;
	}
}
