/**
 * 
 */
package edu.mapred.finalproject;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author arpitm
 *
 */
public class FlightDataGraphMapper extends
		Mapper<Object, Text, Text, Text> {
	// HashMap
	HashMap<String, HashMap<String, Integer>> edgesMap = null;

	// Flight data parser
	FlightDataParser fDataParser = null;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);

		edgesMap = new HashMap<String, HashMap<String, Integer>>();
		fDataParser = FlightDataParser.getInstance();
	}

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// Get flight data object from input
		FlightData fData = fDataParser.getFlightData(value.toString());

		if (fData != null) {

			String currentDate = context.getConfiguration().get("currentDate");
			
			// TODO
			// if (flightDate.equals(FlightConstants.GRAPH_DATE)
			// && (iscancelled == false)) {
			if (FlightUtils.filterDateWithoutDiversionCheck(fData, currentDate)) {
				String source = fData.getOrigin();
				String dest = fData.getDestination();
				String flightDate = fData.getFlightDate();
				boolean isCancelled = fData.isCancelled();
				int actualElapsedTime = fData.getActualElapsedTime();

				if (edgesMap.containsKey(source)) {
					HashMap<String, Integer> destMap = edgesMap.get(source);

					if (destMap.containsKey(dest)) {
						int oldElapsedTime = destMap.get(dest);

						if (oldElapsedTime > actualElapsedTime) {
							destMap.put(dest, actualElapsedTime);
							edgesMap.put(source, destMap);
						}

					} else {
						destMap.put(dest, actualElapsedTime);
						edgesMap.put(source, destMap);
					}
				} else {
					HashMap<String, Integer> destMap = new HashMap<String, Integer>();
					destMap.put(dest, actualElapsedTime);
					edgesMap.put(source, destMap);
				}
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		for (String src : edgesMap.keySet()) {
			HashMap<String, Integer> destinations = edgesMap.get(src);

			for (String destination : destinations.keySet()) {
				Text key = new Text(src);
				Text value = new Text(destination
						+ FlightConstants.DELIMITER
						+ destinations.get(destination));

				// Emit
				context.write(key, value);
			}
		}

		edgesMap = null;

		super.cleanup(context);
	}
}
