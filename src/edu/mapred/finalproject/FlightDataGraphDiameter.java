/**
 * Program calculates the diameter of the Flight Data graph, i.e, the longest shortest path.
 */
package edu.mapred.finalproject;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @author arpitm
 * 
 */
public class FlightDataGraphDiameter {

	/**
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		// bitwise or test
		// FMBitmask bm1 = new FMBitmask();
		// bm1.setBit(0);
		// bm1.setBit(1);
		// bm1.setBit(2);
		//
		// FMBitmask bm2 = new FMBitmask();
		// bm2.setBit(2);
		// bm2.setBit(3);
		// System.out.println("Mask 2: " + bm2.toString());
		//
		// bm2.bitwiseOrWith(bm1);
		//
		// //TODO
		// System.out.println("Mask 1: " + bm1.toString());
		// System.out.println("Result Mask: " + bm2.toString());
		// System.exit(0);

		Configuration genGraphJobConf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(genGraphJobConf, args)
				.getRemainingArgs();

		// Input sanity check
		if (otherArgs.length != 3) {
			System.err
					.println("Usage: GraphDiameter <directory-path> <input-file-name> <MM-DD-YYYY>");
			System.exit(2);
		}

		// Input and output paths
		String dirPath = otherArgs[0];
		String inputFile = otherArgs[1];
		String currentDate = otherArgs[2];

		// Begin from start date
		// int currentDay = Integer.parseInt(FlightConstants.START_DAY);
		// int currentMonth = Integer.parseInt(FlightConstants.START_MONTH);
		// int currentYear = Integer.parseInt(FlightConstants.END_YEAR);
		// int endDay = Integer.parseInt(FlightConstants.END_DAY);
		// int endMonth = Integer.parseInt(FlightConstants.END_MONTH);
		// int endYear = Integer.parseInt(FlightConstants.END_YEAR);

		// while ((currentDay <= endDay) && (currentMonth <= endMonth)
		// && (currentYear <= endYear)) {
		// String currentDate = currentMonth + "-" + currentDay + "-"
		// + currentYear;

		// TODO
		// System.out.println("Current Date = " + currentDate);

		// Configuration conf = new Configuration();
		// TODO Required?
		genGraphJobConf.set("currentDate", currentDate);

		// Job 1: Graph generation
		String genGraphJobName = "Flight Data Graph Generation For Date "
				+ currentDate;
		// TODO
		System.out.println("Starting Job: " + genGraphJobName);

		Job genGraphJob = new Job(genGraphJobConf, genGraphJobName);
		genGraphJob.setJarByClass(FlightDataGraphDiameter.class);
		genGraphJob.setMapperClass(FlightDataGraphMapper.class);
		genGraphJob.setReducerClass(FlightDataGraphReducer.class);
		// genGraphJob.setPartitionerClass(FlightDataGraphPartitioner.class);

		genGraphJob.setMapOutputKeyClass(Text.class);
		genGraphJob.setMapOutputValueClass(Text.class);
		genGraphJob.setOutputKeyClass(NullWritable.class);
		genGraphJob.setOutputValueClass(Text.class);
		String genGraphJobInputPathStr = dirPath + "/input/" + inputFile;
		String genGraphJobOutputPathStr = dirPath + "/output/" + currentDate
				+ "/flight-data-graph/";
		Path genGraphJobInputPath = new Path(genGraphJobInputPathStr);
		Path genGraphJobOutputPath = new Path(genGraphJobOutputPathStr);

		// TODO
		System.out.println("Deleting old output directory "
				+ genGraphJobOutputPathStr);
		FileSystem.getLocal(genGraphJobConf)
				.delete(genGraphJobOutputPath, true);

		FileInputFormat.addInputPath(genGraphJob, genGraphJobInputPath);
		FileOutputFormat.setOutputPath(genGraphJob, genGraphJobOutputPath);
		int genGraphJobCompletionStatus = (genGraphJob.waitForCompletion(true) ? 0
				: 1);

		if (genGraphJobCompletionStatus == 0) {
			// TODO
			System.out.println("Job " + genGraphJobName
					+ " completed successfully!");
			// System.exit(0);

			int iteration = 1;
			boolean hasConverged = false;
			int prevHopN = 0;
			int currHopN = 0;

			while (hasConverged == false) {
				// TODO
				System.out.print("-------- Iteration: " + iteration
						+ " --------");

				// HADI Stage 1: Invert Edge, match bitmasks to node id.
				Configuration hadiStage1Conf = new Configuration();

				if (1 == iteration) {
					hadiStage1Conf.set("bitmaskCommand", "BC");
				} else {
					hadiStage1Conf.set("bitmaskCommand", "B");
				}

				String hadiStage1JobName = "HADI Stage 1";

				// TODO
				System.out.println("Starting Job: " + hadiStage1JobName);

				Job hadiStage1Job = new Job(hadiStage1Conf, hadiStage1JobName);
				hadiStage1Job.setJarByClass(FlightDataGraphDiameter.class);
				hadiStage1Job.setMapperClass(HADIStage1.HADIStage1Mapper.class);
				hadiStage1Job
						.setReducerClass(HADIStage1.HADIStage1Reducer.class);
				// hadiStage1Job.setPartitionerClass(HADIStage1.HADIStage1Partitioner.class);
				hadiStage1Job
						.setGroupingComparatorClass(HADIStage1.HADIStage1GroupComparator.class);
				hadiStage1Job.setMapOutputKeyClass(Text.class);
				hadiStage1Job.setMapOutputValueClass(Text.class);
				hadiStage1Job.setOutputKeyClass(NullWritable.class);
				hadiStage1Job.setOutputValueClass(Text.class);
				String hadiStage1OutputPathStr = dirPath + "/output/"
						+ currentDate + "/iteration-" + iteration
						+ "/hadi-stage1/";
				Path hadiStage1OutputPath = new Path(hadiStage1OutputPathStr);
				FileOutputFormat.setOutputPath(hadiStage1Job,
						hadiStage1OutputPath);

				// TODO
				System.out.println("Deleting old output directory "
						+ hadiStage1OutputPath);
				FileSystem.getLocal(hadiStage1Conf).delete(
						hadiStage1OutputPath, true);

				if (iteration == 1) {
					Path hadiStage1InputPath = new Path(
							genGraphJobOutputPathStr + "/part-r-00000");
					FileInputFormat.addInputPath(hadiStage1Job,
							hadiStage1InputPath);
				} else {
					// TODO
					Path hadiStage1InputPath1 = new Path(
							genGraphJobOutputPathStr + "/part-r-00000");
					Path hadiStage1InputPath2 = new Path(dirPath + "/output/"
							+ currentDate + "/iteration-" + (iteration - 1)
							+ "/hadi-stage2/part-r-00000");
					FileInputFormat.addInputPath(hadiStage1Job,
							hadiStage1InputPath1);
					FileInputFormat.addInputPath(hadiStage1Job,
							hadiStage1InputPath2);
				}

				int hadiS1CompletionStatus = (hadiStage1Job
						.waitForCompletion(true) ? 0 : 1);
				if (hadiS1CompletionStatus == 0) {
					// TODO
					System.out.println("Job: " + hadiStage1JobName
							+ " completed successfully for iteration "
							+ iteration);
					// System.exit(0);

					// HADI Stage 2: Merge bitmasks for each node.
					Configuration hadiStage2Conf = new Configuration();
					String hadiStage2JobName = "HADI Stage 2";

					// TODO
					System.out.println("Starting Job: " + hadiStage2JobName);

					Job hadiStage2Job = new Job(hadiStage2Conf,
							hadiStage2JobName);
					hadiStage2Job.setJarByClass(FlightDataGraphDiameter.class);
					hadiStage2Job
							.setMapperClass(HADIStage2.HADIStage2Mapper.class);
					hadiStage2Job
							.setReducerClass(HADIStage2.HADIStage2Reducer.class);
					// hadiStage2Job
					// .setPartitionerClass(HADIStage2.HADIStage2Partitioner.class);
					hadiStage2Job
							.setGroupingComparatorClass(HADIStage2.HADIStage2GroupComparator.class);
					hadiStage2Job.setMapOutputKeyClass(Text.class);
					hadiStage2Job.setMapOutputValueClass(Text.class);
					hadiStage2Job.setOutputKeyClass(NullWritable.class);
					hadiStage2Job.setOutputValueClass(Text.class);
					String hadiStage2InputPathStr = hadiStage1OutputPathStr
							+ "/part-r-00000";
					Path hadiStage2InputPath = new Path(hadiStage2InputPathStr);
					String hadiStage2OutputPathStr = dirPath + "/output/"
							+ currentDate + "/iteration-" + iteration
							+ "/hadi-stage2/";
					Path hadiStage2OutputPath = new Path(
							hadiStage2OutputPathStr);

					// TODO
					System.out.println("Deleting old output directory "
							+ hadiStage2OutputPathStr);
					FileSystem.getLocal(hadiStage2Conf).delete(
							hadiStage2OutputPath, true);

					FileInputFormat.addInputPath(hadiStage2Job,
							hadiStage2InputPath);
					FileOutputFormat.setOutputPath(hadiStage2Job,
							hadiStage2OutputPath);
					int hadiS2CompletionStatus = (hadiStage2Job
							.waitForCompletion(true) ? 0 : 1);

					if (hadiS2CompletionStatus == 0) {
						// TODO
						System.out.println("Job: " + hadiStage2JobName
								+ " completed successfully for iteration "
								+ iteration);
						// System.exit(-2);

						// HADI Stage 3: Calculation of neighborhood function
						// N(h)
						Configuration hadiStage3Conf = new Configuration();
						hadiStage3Conf.set("currHop",
								Integer.toString(iteration));
						String hadiStage3JobName = "HADI Stage 3";

						// TODO
						System.out
								.println("Starting Job: " + hadiStage3JobName);

						Job hadiStage3Job = new Job(hadiStage3Conf,
								hadiStage3JobName);
						hadiStage3Job
								.setJarByClass(FlightDataGraphDiameter.class);
						hadiStage3Job
								.setMapperClass(HADIStage3.HADIStage3Mapper.class);
						// hadiStage3Job
						// .setPartitionerClass(HADIStage3.HADIStage3Partitioner.class);
						hadiStage3Job
								.setReducerClass(HADIStage3.HADIStage3Reducer.class);
						hadiStage3Job.setMapOutputKeyClass(Text.class);
						hadiStage3Job.setMapOutputValueClass(IntWritable.class);
						hadiStage3Job.setOutputKeyClass(Text.class);
						hadiStage3Job.setOutputValueClass(IntWritable.class);
						String hadiStage3InputPathStr = hadiStage2OutputPathStr;
						Path hadiStage3InputPath = new Path(
								hadiStage3InputPathStr + "/part-r-00000");
						String hadiStage3OutputPathStr = dirPath + "/output/"
								+ currentDate + "/iteration-" + iteration
								+ "/hadi-stage3/";
						Path hadiStage3OutputPath = new Path(
								hadiStage3OutputPathStr);

						// TODO
						System.out.println("Deleting old output directory "
								+ hadiStage3OutputPathStr);
						FileSystem.getLocal(hadiStage3Conf).delete(
								hadiStage3OutputPath, true);

						FileInputFormat.addInputPath(hadiStage3Job,
								hadiStage3InputPath);
						FileOutputFormat.setOutputPath(hadiStage3Job,
								hadiStage3OutputPath);
						int hadiS3CompletionStatus = (hadiStage3Job
								.waitForCompletion(true) ? 0 : 1);

						if (hadiS3CompletionStatus == 0) {
							// TODO
							System.out.println("Job: " + hadiStage3JobName
									+ " completed successfully for iteration "
									+ iteration);
							// System.exit(-2);

							currHopN = (int) hadiStage3Job
									.getCounters()
									.findCounter(
											FlightConstants.FlightDataGraphCounters.PREV_HOP_N_COUNTER)
									.getValue();

							// TODO
							System.out.println("Current hop N = " + currHopN);
							System.out.println("Prev hop N = " + prevHopN);

							// TODO < or <= ?
							if (currHopN <= prevHopN) {
								// TODO
								System.out.println("Has converged");

								hasConverged = true;
							} else {
								iteration++;
								prevHopN = currHopN;
							}

						} else {
							System.err
									.println("JOB STATUS MESSAGE: HADI Stage 3 Job for date "
											+ currentDate
											+ " failed at iteration "
											+ iteration);
							System.exit(-2);
						}
					} else {
						System.err
								.println("JOB STATUS MESSAGE: HADI Stage 2 Job for date "
										+ currentDate
										+ " failed at iteration "
										+ iteration);
						System.exit(-2);
					}
				} else {
					System.err
							.println("JOB STATUS MESSAGE: HADI Stage 1 Job for date "
									+ currentDate
									+ " failed at iteration "
									+ iteration);
					System.exit(-2);

				}
			}
		} else {
			System.err
					.println("JOB STATUS MESSAGE: Graph generation job for date "
							+ currentDate + " failed!");
			System.exit(-2);
		}
	}
}
