package edu.mapred.assign3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author arpitm
 * 
 *         FlightDataMapperKey class
 * 
 *         TODO
 */
public class FlightDataMapperKey implements
		WritableComparable<FlightDataMapperKey> {
	
	private String layoverCodeAndFlightDate;
	private boolean isFirstFlight;
	private int month;

	public FlightDataMapperKey() {
		super();
		set(new String(), -1, false);
	}

	public FlightDataMapperKey(String layoverCodeAndFlightDate, int month, boolean isFirstFlight) {
		super();
		set(layoverCodeAndFlightDate, month, isFirstFlight);
	}
	public void set(String layoverCodeAndFlightDate, int month, boolean isFirstFlight) {
		this.setLayoverCodeAndFlightDate(layoverCodeAndFlightDate);
		this.setFirstFlight(isFirstFlight);
		this.month = month;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.layoverCodeAndFlightDate = in.readUTF();
		this.month = in.readInt();
		this.isFirstFlight = in.readBoolean();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(layoverCodeAndFlightDate);
		out.writeInt(month);
		out.writeBoolean(isFirstFlight);
	}

	@Override
	public int compareTo(FlightDataMapperKey o) {
		int compareResult = 0;
		
		//TODO Test both
		compareResult = new Text(layoverCodeAndFlightDate).compareTo(new Text(o.getLayoverCodeAndFlightDate()));
//		compareResult = layoverCodeAndFlightDate.compareTo(o.getLayoverCodeAndFlightDate());
		if(compareResult != 0) {
			return compareResult;
		}
		
		compareResult = new BooleanWritable(isFirstFlight).compareTo(o.isFirstFlight());
		if(compareResult != 0) {
			return compareResult;
		}
		
		int n1 = this.month;
		int n2 = o.month;
		return ((n1 == n2) ? 0 : (n1 < n2) ? -1 : 1);
	}

	public String getLayoverCodeAndFlightDate() {
		return layoverCodeAndFlightDate;
	}

	public void setLayoverCodeAndFlightDate(String layoverCodeAndFlightDate) {
		this.layoverCodeAndFlightDate = layoverCodeAndFlightDate;
	}

	public boolean isFirstFlight() {
		return isFirstFlight;
	}

	public void setFirstFlight(boolean isFirstFlight) {
		this.isFirstFlight = isFirstFlight;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}
}
