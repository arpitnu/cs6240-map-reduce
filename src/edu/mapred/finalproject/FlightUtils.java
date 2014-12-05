package edu.mapred.finalproject;

/**
 * Util class for Map-Reduce job. Provides helper functions.
 * 
 * @author arpitm
 * 
 */
public class FlightUtils {

	public static boolean filterMonthWithoutDiversionCheck(FlightData fData,
			String date) {
		boolean result = false;
		String[] dateSplits = date.split("-");
		int month = Integer.parseInt(dateSplits[1]);

		if ((fData.getFlightMonth() == month) && (fData.isCancelled() == false)) {
			result = true;
		}

		return result;
	}

	public static boolean filterMonthWithDiversionCheck(FlightData fData,
			String date) {
		boolean result = false;
		String[] dateSplits = date.split("-");
		int month = Integer.parseInt(dateSplits[1]);

		if ((fData.getFlightMonth() == month) && (fData.isCancelled() == false)
				&& (fData.isDiverted() == false)) {
			result = true;
		}

		return result;
	}

	public static boolean filterDateWithoutDiversionCheck(FlightData fData,
			String date) {
		boolean result = false;

		if ((fData.getFlightDate().equals(date))
				&& (fData.isCancelled() == false)) {
			result = true;
		}

		return result;
	}

	public static boolean filterDateWithDiversionCheck(FlightData fData,
			String date) {
		boolean result = false;

		if ((fData.getFlightDate().equals(date))
				&& (fData.isCancelled() == false)
				&& (fData.isDiverted() == false)) {
			result = true;
		}

		return result;
	}

}
