package edu.mapred.assign4.java;

/**
 * @author arpitm
 * 
 *         FlightData
 * 
 *         Class contains the relevant flight data & get and set functions
 * 
 */
public class FlightData {

	/**
	 * The flight year
	 */
	private int flightYear;
	
	/**
	 * The month
	 */
	private int flightMonth;

	/**
	 * The unique carrier
	 */
	private String airlineId;

	/**
	 * Is flight cancelled
	 */
	private boolean isCancelled;

	/**
	 * s flight delayed
	 */
	private boolean isDiverted;

	/**
	 * Arrival delay in minutes
	 */
	private String arrDelay;

	/**
	 * Default constructor
	 */
	public FlightData() {
		setFlightYear(0);
		setFlightMonth(0);
		setCancelled(false);
		setDiverted(false);
		setArrDelay(new String());
		setAirlineId(new String());
	}

	@Override
	public String toString() {
		String str = null;

		str = "Flight Data: [" + "airlineId = " + airlineId + "flightYear = "
				+ flightYear + ", arrDelayMinutes = " + arrDelay
				+ ", isCancelled = "
				+ ((isCancelled == false) ? "false" : "true")
				+ ", isDiverted = "
				+ ((isDiverted == false) ? "false" : "true") + "]";

		return str;
	}

	/*
	 * Getters & Setters
	 */

	public int getFlightYear() {
		return flightYear;
	}

	public void setFlightYear(int flightYear) {
		this.flightYear = flightYear;
	}

	public boolean isCancelled() {
		return isCancelled;
	}

	public void setCancelled(boolean isCancelled) {
		this.isCancelled = isCancelled;
	}

	public boolean isDiverted() {
		return isDiverted;
	}

	public void setDiverted(boolean isDiverted) {
		this.isDiverted = isDiverted;
	}

	public String getArrDelay() {
		return arrDelay;
	}

	public void setArrDelay(String arrDelay) {
		this.arrDelay = arrDelay;
	}

	public String getAirlineId() {
		return airlineId;
	}

	public void setAirlineId(String airlineId) {
		this.airlineId = airlineId;
	}

	public int getFlightMonth() {
		return flightMonth;
	}

	public void setFlightMonth(int flightMonth) {
		this.flightMonth = flightMonth;
	}

}
