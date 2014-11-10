package edu.mapred.assign4.helper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;

import edu.mapred.assign4.java.FlightData;
import edu.mapred.assign4.java.FlightDataParser;

public class GenerateAirlineId {

	public static void main(String[] args) {
		String fileName = args[0];

		HashSet<String> airlineIds = new HashSet<String>();

		BufferedReader br = null;

		FlightDataParser parser = null;
		try {
			parser = FlightDataParser.getInstance();
			br = new BufferedReader(new FileReader(new File(fileName)));
			String line = null;

			// TODO
			System.out.println("Reading data.csv");

			while ((line = br.readLine()) != null) {
				// TODO
//				System.out.println(line);

				FlightData fData = parser.getFlightData(line);
				airlineIds.add(fData.getAirlineId());
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		// TODO
		System.out.println("Writing to file.");

		// output the unique airline ids into the output file
		PrintWriter pw = null;
		try {
			pw = new PrintWriter(new File(args[1]));
			for (String airline : airlineIds) {
				// TODO
				System.out.println(airline);

				pw.println(airline);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			if (pw != null) {
				pw.close();
			}
		}
	}
}
