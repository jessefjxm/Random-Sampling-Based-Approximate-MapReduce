package org.apache.hadoop.mapreduce.rand;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class RandomSamplingUtil {
	private static final Log LOG = LogFactory.getLog(RandomSamplingUtil.class);

	// singleton
	public static RandomSamplingUtil util = null;

	// Configurations
	public static final String SAMPLE_RATIO = "mapreduce.randomsapling.samplingratio";
	public static final String ERROR_BOUNDS = "mapreduce.randomsapling.errorbounds";
	public static final String INPUT_BITS = "mapreduce.randomsapling.inputbits";
	public static final String CONFIDENCE_LEVEL = "mapreduce.randomsapling.conflevel";
	public static final String ZOOKEEPER_URL = "zookeeper.url";
	public static final String ZOOKEEPER_OUTPUT_ROOT_DIR = "zookeeper.output.rootdir";
	public static final String ZOOKEEPER_OUTPUT_PATH = "zookeeper.output.path";
	private Configuration configuration;

	// Parameters
	private double sampleRatio;
	private double errorBounds;
	private double confidenceLevel;
	private int inputbits;
	private int jobid;

	private String zookeeperURL;
	private String zookeeperOutputRootDir;
	private String zookeeperOutputPath;
	private String zookeeperJobsPath;
	private String zookeeperCountPath;
	private String zookeeperResultPath;

	private long startTime;

	// constructor
	public RandomSamplingUtil(Configuration conf) {
		this.configuration = conf;
		// Get the parameters user defined
		sampleRatio = conf.getDouble(SAMPLE_RATIO, 0.1);
		errorBounds = conf.getDouble(ERROR_BOUNDS, 0.02);
		confidenceLevel = conf.getDouble(CONFIDENCE_LEVEL, 0.95);
		inputbits = conf.getInt(INPUT_BITS, 1);

		zookeeperURL = conf.getStrings(ZOOKEEPER_URL, "localhost")[0];
		zookeeperOutputRootDir = conf.getStrings(ZOOKEEPER_OUTPUT_ROOT_DIR, "/mapreduce/rand")[0];
		zookeeperJobsPath = zookeeperOutputRootDir + "/jobs";
		zookeeperOutputPath = conf.getStrings(ZOOKEEPER_OUTPUT_PATH, zookeeperOutputRootDir + "/job_unknown")[0];
		zookeeperCountPath = zookeeperOutputPath + "/count";
		zookeeperResultPath = zookeeperOutputPath + "/result";

		LOG.info("samplingRatio = " + sampleRatio);
		LOG.info("errorBounds = " + errorBounds);
		LOG.info("confidenceLevel = " + confidenceLevel);
		LOG.info("zookeeperURL = " + zookeeperURL);
		LOG.info("zookeeperOutputRootDir = " + zookeeperOutputRootDir);

		// given sampling rate
		if (sampleRatio > 0 && sampleRatio <= 1) {
		} else if (errorBounds < 0 || errorBounds > 1 || confidenceLevel < 0 || confidenceLevel > 1) {
			sampleRatio = 0.1;
		} else { // give error bounds + conf level
			sampleRatio = getSampleRatio(confidenceLevel, errorBounds, Math.pow(10, inputbits));
		}
	}

	// Getter
	public static RandomSamplingUtil get(Configuration conf) {
		if (util == null)
			util = new RandomSamplingUtil(conf);
		return util;
	}

	public double getConfidenceLevel() {
		return confidenceLevel;
	}

	public double getErrorBounds() {
		return errorBounds;
	}

	public double getSamplingRatio() {
		return sampleRatio;
	}

	public static double getSamplingRatio(Configuration conf) {
		return conf.getDouble(SAMPLE_RATIO, 0.1);
	}

	public int getInputbits() {
		return inputbits;
	}

	public String getZookeeperURL() {
		return zookeeperURL;
	}

	public String getZookeeperOutputRootDir() {
		return zookeeperOutputRootDir;
	}

	public String getZookeeperCountPath() {
		return zookeeperCountPath;
	}

	public String getZookeeperOutputPath() {
		return zookeeperOutputPath;
	}

	// Setter
	public static RandomSamplingUtil setupUtil(Configuration conf, String arg1, String arg2, String arg3) {
		// set up config
		// format: [ratio/confLevel,errorRate,bitsOfInputLength] [zookeeperUrl]
		// [zookeeperRootDir]
		if (arg1 != null) {
			String[] strs = arg1.split(",");
			if (strs.length > 1) {
				double confidence = Double.parseDouble(strs[0]);
				double errorrate = Double.parseDouble(strs[1]);
				int bits = Integer.parseInt(strs[2]);

				setConfidenceLevel(conf, confidence);
				setErrorBounds(conf, errorrate);
				setInputBits(conf, bits);
				setSamplingRatio(conf, -1);
			} else {
				setSamplingRatio(conf, Double.parseDouble(strs[0]));
			}
		}
		if (arg2 != null)
			setZookeeperURL(conf, arg2);
		if (arg3 != null)
			setZookeeperOutputRootDir(conf, arg3);

		// do init check
		RandomSamplingUtil util = RandomSamplingUtil.get(conf);
		util.checkCountZK();
		return util;
	}

	public static void setSamplingRatio(Configuration conf, double samplingRatio) {
		conf.setDouble(SAMPLE_RATIO, samplingRatio);
	}

	public static void setConfidenceLevel(Configuration conf, double confidenceLevel) {
		conf.setDouble(CONFIDENCE_LEVEL, confidenceLevel);
	}

	public static void setErrorBounds(Configuration conf, double errorBounds) {
		conf.setDouble(ERROR_BOUNDS, errorBounds);
	}

	public static void setInputBits(Configuration conf, int bits) {
		conf.setInt(INPUT_BITS, bits);
	}

	public static void setZookeeperURL(Configuration conf, String zookeeperURL) {
		conf.setStrings(ZOOKEEPER_URL, zookeeperURL);
	}

	public static void setZookeeperOutputRootDir(Configuration conf, String zookeeperOutputRootDir) {
		conf.setStrings(ZOOKEEPER_OUTPUT_ROOT_DIR, zookeeperOutputRootDir);
	}

	// Zookeeper basic operation
	public ZooKeeper connectZK() {
		ZooKeeper zoo = null;
		try {
			zoo = new ZooKeeper(zookeeperURL, 5000, new Watcher() {
				public void process(WatchedEvent we) {
					if (we.getState() == KeeperState.SyncConnected) {
						// good
					}
				}
			});
		} catch (IOException e) {
			LOG.error(e);
		}
		return zoo;
	}

	public void createZK(ZooKeeper zk, String path, String str) {
		byte[] data = str == null ? null : str.getBytes();
		try {
			zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			LOG.error(e);
		} catch (InterruptedException e) {
			LOG.error(e);
		}
	}

	public Stat existZK(ZooKeeper zk, String path) {
		try {
			return zk.exists(path, false);
		} catch (KeeperException e) {
			LOG.error(e);
		} catch (InterruptedException e) {
			LOG.error(e);
		}
		return null;
	}

	public String getDataZK(ZooKeeper zk, String path) {
		byte[] bn;
		String data = null;
		try {
			bn = zk.getData(path, false, null);
			data = new String(bn, "UTF-8");
		} catch (KeeperException e) {
			LOG.error(e);
		} catch (InterruptedException e) {
			LOG.error(e);
		} catch (UnsupportedEncodingException e) {
			LOG.error(e);
		}
		return data;
	}

	public void setDataZK(ZooKeeper zk, String path, String str) {
		byte[] data = str.getBytes();
		try {
			zk.setData(path, data, zk.exists(path, true).getVersion());
		} catch (KeeperException e) {
			LOG.error(e);
		} catch (InterruptedException e) {
			LOG.error(e);
		}
	}

	public void deleteDataZK(ZooKeeper zk, String path) {
		try {
			zk.delete(path, zk.exists(path, true).getVersion());
		} catch (InterruptedException e) {
			LOG.error(e);
		} catch (KeeperException e) {
			LOG.error(e);
		}
	}

	public void closeZK(ZooKeeper zk) {
		try {
			zk.close();
		} catch (InterruptedException e) {
			LOG.error(e);
		}
	}

	// Zookeeper advanced operation
	public void checkCountZK() {
		ZooKeeper zk = connectZK();
		if (existZK(zk, zookeeperOutputRootDir) == null) {
			createZK(zk, zookeeperOutputRootDir, null);
		}
		LOG.info(zookeeperJobsPath);
		if (existZK(zk, zookeeperJobsPath) == null) {
			jobid = 1;
			createZK(zk, zookeeperJobsPath, Integer.toString(jobid));
		} else {
			jobid = Integer.parseInt(getDataZK(zk, zookeeperJobsPath)) + 1;
			setDataZK(zk, zookeeperJobsPath, Integer.toString(jobid));
		}
		zookeeperOutputPath = zookeeperOutputRootDir + "/job" + jobid;
		configuration.set(ZOOKEEPER_OUTPUT_PATH, zookeeperOutputPath);
		createZK(zk, zookeeperOutputPath, null);

		zookeeperCountPath = zookeeperOutputPath + "/count";
		createZK(zk, zookeeperCountPath, Integer.toString(0));

		zookeeperResultPath = zookeeperOutputPath + "/result";
		closeZK(zk);

		startTime = System.currentTimeMillis();
	}

	public int updateCountZK(ZooKeeper zk) {
		int count = Integer.valueOf(getDataZK(zk, zookeeperCountPath));
		setDataZK(zk, zookeeperCountPath, Integer.toString(count + 1));
		return count;
	}

	public void writeDataZK(ZooKeeper zk, int count, long total, long sampled) {
		String path = zookeeperOutputPath + "/" + count;
		String data = sampled + "/" + total;
		if (existZK(zk, path) == null) {
			createZK(zk, path, data);
		} else {
			setDataZK(zk, path, data);
		}
	}

	public String getSamplingResult() {
		long endTime = System.currentTimeMillis();
		ZooKeeper zk = connectZK();
		int jobs = Integer.parseInt(getDataZK(zk, zookeeperCountPath));
		long total = 0, sample = 0;
		for (int i = 0; i < jobs; i++) {
			String[] res = getDataZK(zk, zookeeperOutputPath + "/" + i).split("/");
			sample += Long.parseLong(res[0]);
			total += Long.parseLong(res[1]);
		}
		double errorrate = getActualErrorRate(confidenceLevel, total, sample);

		String result = "[Job ID] " + jobid + "\n" + "[Total record] " + total + "\n" + "[Sampled record] " + sample
				+ "\n" + "[Designed sample rate] " + String.format("%.2f", sampleRatio * 100) + "%" + "\n"
				+ "[Actual sample rate] " + String.format("%.2f", sample * 1.0 / total * 100) + "%" + "\n"
				+ "[Confidence level] " + confidenceLevel + "\n" + "[Error rate] "
				+ String.format("%.2f", errorrate * 100) + "%" + "\n" + "[Time cost] "
				+ String.format("%.2f", (endTime - startTime) * 1.0 / 1000.0) + "s";
		createZK(zk, zookeeperResultPath, result);

		closeZK(zk);
		return result;
	}

	// Calculations
	private static double getZScore(double confidence) {
		if (confidence == 0) {
			return 0.0;
		} else if (confidence <= 0.05) {
			return 0.0627;
		} else if (confidence <= 0.10) {
			return 0.1257;
		} else if (confidence <= 0.15) {
			return 0.1891;
		} else if (confidence <= 0.20) {
			return 0.2533;
		} else if (confidence <= 0.25) {
			return 0.3186;
		} else if (confidence <= 0.30) {
			return 0.3853;
		} else if (confidence <= 0.40) {
			return 0.5244;
		} else if (confidence <= 0.50) {
			return 0.67;
		} else if (confidence <= 0.60) {
			return 0.84;
		} else if (confidence <= 0.70) {
			return 1.04;
		} else if (confidence <= 0.75) {
			return 1.15;
		} else if (confidence <= 0.80) {
			return 1.28;
		} else if (confidence <= 0.85) {
			return 1.44;
		} else if (confidence <= 0.90) {
			return 1.645;
		} else if (confidence <= 0.95) {
			return 1.96;
		} else if (confidence <= 0.98) {
			return 2.33;
		} else if (confidence <= 0.99) {
			return 2.575;
		} else if (confidence <= 0.995) {
			return 2.81;
		} else if (confidence <= 0.999) {
			return 3.09;
		}
		return 5.0;
	}

	private static double getActualErrorRate(double confidence, long total, long sample) {
		return Math.sqrt((0.25 / sample)) * getZScore(confidence) * Math.sqrt((double) (total - sample) / (total - 1));
	}

	private static double getSampleRatio(double confidence, double errorrate, double total) {
		return 1.0 / (1.0 + 4 * total * Math.pow(errorrate, 2) / Math.pow(getZScore(confidence), 2));
	}
}
