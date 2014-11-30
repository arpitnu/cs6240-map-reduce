/**
 * Program counts the total number of airport Id's
 */
package edu.mapred.finalproject;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

/**
 * @author arpitm
 * 
 */
public class NodesCount {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String filePath = args[0];
		BufferedReader br = null;
		String line = null;
		FlightDataParser dataParser = FlightDataParser.getInstance();
		ArrayList<String> listAirportIds = new ArrayList<String>();
		FlightData fData = null;

		try {
			br = new BufferedReader(new FileReader(filePath));
			while (null != (line = br.readLine())) {
				fData = dataParser.getFlightData(line);

				if (fData != null) {

					String origin = fData.getOrigin();
					String dest = fData.getDestination();

					if (!listAirportIds.contains(origin)) {
						//TODO
						System.out.println("Adding " + origin + " to list.");
						
						listAirportIds.add(origin);
					}

					if (!listAirportIds.contains(dest)) {
						//TODO
						System.out.println("Adding " + dest + " to list.");
						
						listAirportIds.add(dest);
					}
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("Num of ids = " + listAirportIds.size());
	}

}
