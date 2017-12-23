package org.apache.hadoop.examples;

import java.io.IOException;
import java.net.URLDecoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.rand.RandomSamplingUtil;
import org.apache.hadoop.mapreduce.rand.RandomizedTextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

public class RandomizedCharacterCount {
	private static final Logger LOG = Logger.getLogger(RandomizedCharacterCount.class);

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			char[] chars = value.toString().toCharArray();
			for (char c : chars) {
				word.set(Character.toString(c));
				context.write(word, one);
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
			System.err.println("Usage: randcharcount <in> <out> [ratio/confLevel,errorRate,bitsOfInputLength] "
					+ "[zookeeperUrl] [zookeeperRootDir]");
			System.out.println("Example: randcharcount input output 0.1");
			System.out.println("Example: randcharcount input output 0.95,0.01,8");
			System.exit(2);
		}
		// setup the randomization util with user specific arguments
		RandomSamplingUtil util = RandomSamplingUtil.setupUtil(conf, otherArgs.length >= 3 ? otherArgs[2] : null,
				otherArgs.length >= 4 ? otherArgs[3] : null, otherArgs.length >= 5 ? otherArgs[4] : null);

		// setup job
		Job job = Job.getInstance(conf, "ra:qndcharcount");
		job.setJarByClass(RandomizedCharacterCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// use random sampled input
		job.setInputFormatClass(RandomizedTextInputFormat.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);

		RandomizedTextInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		job.waitForCompletion(true);

		// Check result of this job
		System.out.println(util.getSamplingResult());
	}
}
