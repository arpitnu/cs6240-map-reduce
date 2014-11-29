package edu.mapreduce.finalproject;

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
	 * The flight date
	 */
	private String flightDate;

	/**
	 * The flight month
	 */
	private int flightMonth;

	/**
	 * The flight year
	 */
	private int flightYear;

	/**
	 * The origin code
	 */
	private String origin;

	/**
	 * The destination code
	 */
	private String destination;

	/**
	 * Is flight cancelled
	 */
	private boolean isCancelled;

	/**
	 * Actual elapsed time of flight
	 */
	private int actualElapsedTime;

	/**
	 * Default constructor
	 */
	public FlightData() {
		setFlightDate(new String());
		setFlightMonth(0);
		setFlightYear(0);
		setCancelled(false);
		setOrigin(new String());
		setDestination(new String());
		setActualElapsedTime(actualElapsedTime);
	}

	@Override
	public String toString() {
		String str = null;

		str = "Flight Data: [" + "flightYear = " + flightYear
				+ ", flightMonth = " + flightMonth + ", flightDate: "
				+ flightDate + ", origin = " + origin + ", destination = "
				+ destination + ", actualElapsedTime = " + actualElapsedTime
				+ ((isCancelled == false) ? "no" : "yes") + ", isDiverted = ";

		return str;
	}

	/*
	 * Getters & Setters
	 */
	public String getFlightDate() {
		return flightDate;
	}

	public void setFlightDate(String flightDate) {
		this.flightDate = flightDate;
	}

	public int getFlightMonth() {
		return flightMonth;
	}

	public void setFlightMonth(int flightMonth) {
		this.flightMonth = flightMonth;
	}

	public int getFlightYear() {
		return flightYear;
	}

	public void setFlightYear(int flightYear) {
		this.flightYear = flightYear;
	}

	public String getOrigin() {
		return origin;
	}

	public void setOrigin(String origin) {
		this.origin = origin;
	}

	public String getDestination() {
		return destination;
	}

	public void setDestination(String destination) {
		this.destination = destination;
	}

	public boolean isCancelled() {
		return isCancelled;
	}

	public void setCancelled(boolean isCancelled) {
		this.isCancelled = isCancelled;
	}

	public int getActualElapsedTime() {
		return actualElapsedTime;
	}

	public void setActualElapsedTime(int actualElapsedTime) {
		this.actualElapsedTime = actualElapsedTime;
	}

}
