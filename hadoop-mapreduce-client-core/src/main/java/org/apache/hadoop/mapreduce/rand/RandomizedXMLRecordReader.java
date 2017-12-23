package org.apache.hadoop.mapreduce.rand;

import java.io.InputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

import java.util.Random;

class RandomizedXMLRecordReader extends RecordReader<LongWritable, Text> {
	private static final Logger LOG = Logger.getLogger(RandomizedXMLRecordReader.class);

	private byte[] startTag;
	private byte[] endTag;
	private long start;
	private long end;
	private long pos;
	private InputStream fsin = null;
	private DataOutputBuffer buffer = new DataOutputBuffer();
	private long recordStartPos;

	private final LongWritable key = new LongWritable();
	private final Text value = new Text();

	// Configurations
	private RandomSamplingUtil util;
	private CompressionCodec codec = null;
	private Decompressor decompressor = null;

	// Parameters
	private long pages = 0;
	private long sampled = 0;
	private double ratio = 0;
	private String START_TAG = "<page>";
	private String END_TAG = "</page>";

	// random generator
	private Random rnd;

	@Override
	public void initialize(InputSplit input, TaskAttemptContext context) throws IOException, InterruptedException {
		rnd = new Random();
		util = RandomSamplingUtil.get(context.getConfiguration());
		ratio = util.getSamplingRatio();

		Configuration conf = context.getConfiguration();

		startTag = START_TAG.getBytes("utf-8");
		endTag = END_TAG.getBytes("utf-8");

		FileSplit split = (FileSplit) input;
		start = split.getStart();
		end = start + split.getLength();
		Path file = split.getPath();

		CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(conf);
		codec = compressionCodecs.getCodec(file);

		FileSystem fs = file.getFileSystem(conf);

		if (isCompressedInput()) {
			LOG.info("Reading compressed file " + file + "...");
			FSDataInputStream fileIn = fs.open(file);
			decompressor = CodecPool.getDecompressor(codec);
			if (codec instanceof SplittableCompressionCodec) {
				// We can read blocks
				final SplitCompressionInputStream cIn = ((SplittableCompressionCodec) codec).createInputStream(fileIn,
						decompressor, start, end, SplittableCompressionCodec.READ_MODE.BYBLOCK);
				fsin = cIn;
				start = cIn.getAdjustedStart();
				end = cIn.getAdjustedEnd();
			} else {
				// We cannot read blocks, we have to read everything
				fsin = new DataInputStream(codec.createInputStream(fileIn, decompressor));

				end = Long.MAX_VALUE;
			}
		} else {
			LOG.info("Reading uncompressed file " + file + "...");
			FSDataInputStream fileIn = fs.open(file);

			fileIn.seek(start);
			fsin = fileIn;

			end = start + split.getLength();
		}

		recordStartPos = start;

		pos = start;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (getFilePosition() < end) {
			// skip a few pages
			while (rnd.nextDouble() > ratio) {
				if (!readUntilMatch(startTag, false)) {
					return false;
				} else {
					pages++;
				}
			}
			sampled++;

			if (readUntilMatch(startTag, false)) {
				pages++;
				recordStartPos = pos - startTag.length;

				try {
					buffer.write(startTag);
					if (readUntilMatch(endTag, true)) {
						key.set(recordStartPos);
						value.set(buffer.getData(), 0, buffer.getLength());
						return true;
					}
				} finally {
					if (fsin instanceof Seekable) {
						if (!isCompressedInput()) {
							if (pos != ((Seekable) fsin).getPos()) {
								throw new RuntimeException("bytes consumed error!");
							}
						}
					}

					buffer.reset();
				}
			}
		}
		return false;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public void close() throws IOException {
		ZooKeeper zk = util.connectZK();
		if (zk != null) {
			int count = util.updateCountZK(zk);
			util.writeDataZK(zk, count, pages, sampled);
		}
		util.closeZK(zk);
		fsin.close();
	}

	@Override
	public float getProgress() throws IOException {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (getFilePosition() - start) / (float) (end - start));
		}
	}

	private boolean isCompressedInput() {
		return (codec != null);
	}

	protected long getFilePosition() throws IOException {
		long retVal;
		if (isCompressedInput() && null != fsin && fsin instanceof Seekable) {
			retVal = ((Seekable) fsin).getPos();
		} else {
			retVal = pos;
		}
		return retVal;
	}

	private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
		int i = 0;
		while (true) {
			int b = fsin.read();

			// end of file:
			if (b == -1)
				return false;

			// increment position (bytes consumed)
			pos++;

			// save to buffer:
			if (withinBlock)
				buffer.write(b);

			// check if we're matching:
			if (b == match[i]) {
				i++;
				if (i >= match.length)
					return true;
			} else
				i = 0;
			// see if we've passed the stop point:
			if (!withinBlock && i == 0 && getFilePosition() >= end)
				return false;
		}
	}
}