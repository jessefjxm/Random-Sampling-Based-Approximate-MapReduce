package org.apache.hadoop.examples;

import java.io.IOException;

import java.util.StringTokenizer;
import java.util.Date;

import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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

public class RandomizedApacheLogAnalysis {

	public static class LogMapper extends Mapper<Object, Text, Text, LongWritable> {
		private final static LongWritable one = new LongWritable(1);
		private Text word = new Text();
		private SimpleDateFormat indateformat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				StringTokenizer matcher = new StringTokenizer(value.toString());
				String hostname = matcher.nextToken();
				matcher.nextToken(); // eat the "-"
				matcher.nextToken("["); // again
				String datetime = matcher.nextToken("]").substring(1);
				matcher.nextToken("\"");
				String request = matcher.nextToken("\"");
				matcher.nextToken(" "); // again
				String response = matcher.nextToken();

				String byteCount = null;
				if (matcher.hasMoreTokens()) {
					byteCount = matcher.nextToken();
				}
				// matcher.nextToken("\"");
				// String referer = matcher.nextToken("\"");
				// matcher.nextToken("\""); // again
				// String userAgent = matcher.nextToken("\"");

				String task = context.getConfiguration().get("task");
				if (task != null) {
					// Check who is trying to hack us
					if (task.equalsIgnoreCase("hack")) {
						// "POST /cgi-bin/php-cgi?XXX HTTP/1.1"
						String[] requestSplit = request.split(" ");
						if (requestSplit.length > 1) {
							String address = requestSplit[1];
							String[] keywords = { "/w00tw00t", "/phpMyAdmin", "/pma", "/myadmin", "/MyAdmin",
									"/phpTest", "/cgi-bin/php", "/cgi-bin/php5", "/cgi-bin/php-cgi" };
							boolean found = false;
							for (String keyword : keywords) {
								if (address.startsWith(keyword)) {
									found = true;
									break;
								}
							}
							// We note that that user is pushing us
							if (found) {
								word.set(hostname);
								context.write(word, one);
							}
						}
						// Hosts visiting the web
					} else if (task.equalsIgnoreCase("host")) {
						word.set(hostname);
						context.write(word, one);
						// Analyze the access time of the visits
						// 24/Nov/2013:06:25:45 -0500 -> Date
					} else if (task.equalsIgnoreCase("dateweek")) {
						Date date = indateformat.parse(datetime);
						SimpleDateFormat outdateformat = new SimpleDateFormat("EEE HH"); // Day of the week
						word.set(outdateformat.format(date));
						context.write(word, one);
						// Size per object
					} else if (task.equalsIgnoreCase("size")) {
						long bytes = (Long.parseLong(byteCount) / 100) * 100; // Round for histogram
						word.set(Long.toString(bytes));
						context.write(word, one);
						// Total size per object
					} else if (task.equalsIgnoreCase("totalsize")) {
						word.set("Total");
						context.write(word, new LongWritable(Long.parseLong(byteCount)));
						// Page traffic
					} else if (task.equalsIgnoreCase("pagesize")) {
						int pos1 = request.indexOf(" "), pos2 = request.indexOf("?"), pos3 = request.lastIndexOf(" ");
						String aux = request.substring(pos1, pos2 <= pos1 ? pos3 : pos2);
						word.set(aux);
						context.write(word, new LongWritable(Long.parseLong(byteCount)));
						// Page visit
					} else if (task.equalsIgnoreCase("page")) {
						int pos1 = request.indexOf(" "), pos2 = request.indexOf("?"), pos3 = request.lastIndexOf(" ");
						String aux = request.substring(pos1, pos2 <= pos1 ? pos3 : pos2);
						word.set(aux);
						context.write(word, one);
						// Default
					} else {
						System.err.println("Unknown option:" + task);
					}
				}
			} catch (Exception ee) {
				// ee.printStackTrace();
			}
		}
	}

	public static class LongSumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();

		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable val : values) {
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
			System.err.println("Usage: randapachelog <in> <out> <task> [ratio/confLevel,errorRate,bitsOfInputLength] "
					+ "[zookeeperUrl] [zookeeperRootDir]");
			System.out.println("hack - Check who is trying to hack us");
			System.out.println("host - Hosts visiting the web");
			System.out.println("dateweek - Analyze the access time of the visits");
			System.out.println("size - Size per object");
			System.out.println("totalsize - Total size per object");
			System.out.println("pagesize - Page traffic");
			System.out.println("page - Page visit");
			System.out.println("Example: randapachelog input output pagesize 0.1");
			System.out.println("Example: randapachelog input output dateweek 0.95,0.01,8");
			System.exit(2);
		}
		// setup the randomization util with user specific arguments
		conf.setStrings("task", args[2]);
		RandomSamplingUtil util = RandomSamplingUtil.setupUtil(conf, otherArgs.length >= 4 ? otherArgs[3] : null,
				otherArgs.length >= 5 ? otherArgs[4] : null, otherArgs.length >= 6 ? otherArgs[5] : null);

		Job job = Job.getInstance(conf, "Apache log analysis");

		job.setJarByClass(RandomizedApacheLogAnalysis.class);

		job.setMapperClass(LogMapper.class);
		job.setCombinerClass(LongSumReducer.class);
		job.setReducerClass(LongSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setInputFormatClass(RandomizedTextInputFormat.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		// Check result of this job
		job.waitForCompletion(true);
		System.out.println(util.getSamplingResult());
	}
}
