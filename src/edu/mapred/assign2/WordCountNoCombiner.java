/**
 * Word count program with custom partitioner.
 */
package edu.mapred.assign2;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Class: WordCountNoCombiner
 * 
 * @author arpitm
 *
 */
public class WordCountNoCombiner {

	/**
	 * Mapper Class
	 */
	public static class WordCountMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());

			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				
				char first_char = word.toString().toLowerCase().charAt(0);

				// Check if "real" word. Lower case first character between 'm' & 'q'
				if ((first_char >= 'm') && (first_char <= 'q')) {
					context.write(word, one);
				}
			}
		}
	}

	/**
	 * Reducer Class
	 */
	public static class WordCountReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	/**
	 * Partitioner Class
	 */
	public static class WordPartitioner extends Partitioner<Text, IntWritable> {

		@Override
		public int getPartition(Text key, IntWritable value, int numPartitions) {
			char first_character = key.toString().toLowerCase().charAt(0);
			
			// Return the offset of first character of word from 'm' as the partition number.
			return (first_character - 'm');
		}		
	}

	/**
	 * main: driver function
	 * 
	 * @param args 
	 * 			list of arguments for the Job - Input file & output directory.
	 * 
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		
		if (otherArgs.length != 2) {
			System.err.println("Usage: WordCountNoCombiner <in> <out>");
			System.exit(2);
		}
		
		/**
		 * The Job
		 */
		Job job = new Job(conf, "WordCount with custom partitioner.");
		
		/**
		 * Set the Jar by finding where a given class came from. 
		 */
		job.setJarByClass(WordCountNoCombiner.class);
		
		/**
		 * Set Mapper Class
		 */
		job.setMapperClass(WordCountMapper.class);
		
		/**
		 * Set Reducer Class
		 */
		job.setReducerClass(WordCountReducer.class);
		
		/**
		 * Note: No Combiner Class Set.
		 */
		//job.setCombinerClass(WordCountReducer.class);
		
		/**
		 * Set the number of reduce tasks for the job. 
		 */
		job.setNumReduceTasks(5);
		
		/**
		 * Set Partitioner Class
		 */
		job.setPartitionerClass(WordPartitioner.class);
		
		/**
		 * Set the key class for the job output data. 
		 */
		job.setOutputKeyClass(Text.class);
		
		/**
		 * Set the value class for job outputs.
		 */
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}