package edu.mapred.assign4;

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
	 * s flight delayed
	 */
	private boolean isDiverted;

	/**
	 * Departure Time
	 */
	private String depTime;

	/**
	 * Arrival Time
	 */
	private String arrTime;

	/**
	 * Arrival delay in minutes
	 */
	private String arrDelay;

	/**
	 * Default constructor
	 */
	public FlightData() {
		setFlightDate(new String());
		setFlightMonth(0);
		setFlightYear(0);
		setCancelled(false);
		setDiverted(false);
		setOrigin(new String());
		setDestination(new String());
		setDepTime(new String());
		setArrTime(new String());
		setArrDelay(new String());
	}

	@Override
	public String toString() {
		String str = null;

		str = "Flight Data: [" + "flightYear = " + flightYear
				+ ", flightMonth = " + flightMonth + ", flightDate: "
				+ flightDate + ", origin = " + origin + ", destination = "
				+ destination + ", departureTime = " + depTime
				+ ", arrivalTime = " + arrTime + ", arrDelayMinutes = "
				+ arrDelay + ", isCancelled = "
				+ ((isCancelled == false) ? "no" : "yes") + ", isDiverted = "
				+ ((isDiverted == false) ? "no" : "yes") + "]";

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

	public boolean isDiverted() {
		return isDiverted;
	}

	public void setDiverted(boolean isDiverted) {
		this.isDiverted = isDiverted;
	}

	public String getDepTime() {
		return depTime;
	}

	public void setDepTime(String depTime) {
		this.depTime = depTime;
	}

	public String getArrTime() {
		return arrTime;
	}

	public void setArrTime(String arrTime) {
		this.arrTime = arrTime;
	}

	public String getArrDelay() {
		return arrDelay;
	}

	public void setArrDelay(String arrDelay) {
		this.arrDelay = arrDelay;
	}

}
