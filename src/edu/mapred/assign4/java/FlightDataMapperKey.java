/**
 * 
 */
package edu.mapred.assign4.java;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * @author arpitm
 * 
 */
public class FlightDataMapperKey implements
		WritableComparable<FlightDataMapperKey> {
	private String airlineId;
	private int month;

	public FlightDataMapperKey() {
	}

	public FlightDataMapperKey(String airlineId, int month) {
		set(airlineId, month);
	}

	private void set(String airlineId, int month) {
		this.airlineId = airlineId;
		this.month = month;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.airlineId = in.readUTF();
		this.month = in.readInt();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.airlineId);
		out.writeInt(this.month);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(FlightDataMapperKey key) {
		int cmpResult = this.airlineId.compareTo(key.airlineId);

		if (cmpResult == 0) {
			cmpResult = compareMonths(this.month, key.getMonth());
		}

		return cmpResult;
	}

	@Override
	public boolean equals(Object obj) {
		boolean isEqual = false;

		if (obj instanceof FlightDataMapperKey) {
			FlightDataMapperKey key = (FlightDataMapperKey) obj;
			isEqual = ((this.airlineId.equals(key.airlineId)) && (this.month == key.getMonth()));
		}

		return isEqual;
	}

	public static int compareMonths(int m1, int m2) {
		return ((m1 < m2) ? -1 : ((m1 > m2) ? 1 : 0));
	}

	@Override
	public String toString() {
		return this.airlineId.toString() + FlightConstants.DELIMITER
				+ this.month;
	}

	/**
	 * Source: http://zaloni.com/blog/secondary-sorting-hadoop
	 */
	@Override
	public int hashCode() {
		int resultHash = 1;
		int prime = 31;

		resultHash = prime * resultHash
				+ ((this.airlineId != null) ? this.airlineId.hashCode() : 0);

		resultHash = prime * resultHash + this.month;

		return resultHash;
	}

	/*
	 * Get and set functions for the key components
	 */
	public String getAirlineId() {
		return this.airlineId;
	}

	public void setAirlineId(String airlineId) {
		this.airlineId = airlineId;
	}

	public int getMonth() {
		return this.month;
	}

	public void setMonth(int month) {
		this.month = month;
	}
}
