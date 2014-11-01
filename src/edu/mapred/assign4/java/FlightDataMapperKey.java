/**
 * 
 */
package edu.mapred.assign4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author arpitm
 * 
 */
public class FlightDataKey implements WritableComparable<FlightDataKey> {
	private Text layoverCode;
	private Text airlineCarrier;
	private Text flightDate;

	public FlightDataKey() {
		set(new Text(), new Text(), new Text());
	}

	public FlightDataKey(Text layoverCode, Text flightDate, Text airlineId) {
		set(layoverCode, flightDate, airlineId);
	}

	private void set(Text layoverCode, Text flightDate, Text airlineId) {
		this.layoverCode = layoverCode;
		this.airlineCarrier = airlineId;
		this.flightDate = flightDate;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		layoverCode.readFields(in);
		flightDate.readFields(in);
		airlineCarrier.readFields(in);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		layoverCode.write(out);
		flightDate.write(out);
		airlineCarrier.write(out);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(FlightDataKey o) {
		int cmpResult = layoverCode.compareTo(o.layoverCode);

		if (cmpResult == 0) {
			cmpResult = flightDate.compareTo(o.flightDate);

			// TODO
			// if(cmpResult == 0) {
			// cmpResult = airlineCarrier.compareTo(o.airlineCarrier);
			// }
		}

		return cmpResult;
	}

	@Override
	public boolean equals(Object obj) {
		boolean isEqual = false;

		if (obj instanceof FlightDataKey) {
			FlightDataKey k = (FlightDataKey) obj;
			isEqual = ((layoverCode.equals(k.layoverCode))
					&& (flightDate.equals(k.flightDate)) && (airlineCarrier
					.equals(k.airlineCarrier)));
		}

		return isEqual;
	}

	@Override
	public String toString() {
		return layoverCode.toString() + " " + airlineCarrier.toString() + " "
				+ flightDate.toString();
	}
}
