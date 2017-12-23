package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// Input format
import org.apache.hadoop.mapreduce.rand.RandomSamplingUtil;
import org.apache.hadoop.mapreduce.rand.RandomizedTextInputFormat;

public class RandomizedWirelessLogAnalysis {
	public static class LogMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// "Offset (ms)","Receiver Timestamp (ms)","Physical Layer","Device
			// ID","Receiver ID","RSSI","Received Data"
			StringTokenizer matcher = new StringTokenizer(value.toString());
			try {
				String sndId = matcher.nextToken(",");
				String rcvId = matcher.nextToken(",");

				// Analyze communications
				if (sndId.compareTo(rcvId) > 0) {
					word.set(sndId + "->" + rcvId);
				} else {
					word.set(rcvId + "->" + sndId);
				}
				context.write(word, one);
			} catch (Exception e) {
				System.out.println(e);
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: randwireless <in> <out> [ratio/confLevel,errorRate,bitsOfInputLength] "
					+ "[zookeeperUrl] [zookeeperRootDir]");
			System.out.println("Example: randwireless input output 0.1");
			System.out.println("Example: randwireless input output 0.95,0.01,8");
			System.exit(2);
		}
		// setup the randomization util with user specific arguments
		RandomSamplingUtil util = RandomSamplingUtil.setupUtil(conf, otherArgs.length >= 3 ? otherArgs[2] : null,
				otherArgs.length >= 4 ? otherArgs[3] : null, otherArgs.length >= 5 ? otherArgs[4] : null);

		Job job = Job.getInstance(conf, "wireless log analysis");
		job.setJarByClass(RandomizedWirelessLogAnalysis.class);
		job.setMapperClass(LogMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(RandomizedTextInputFormat.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		// Check result of this job
		job.waitForCompletion(true);
		System.out.println(util.getSamplingResult());
	}
}
