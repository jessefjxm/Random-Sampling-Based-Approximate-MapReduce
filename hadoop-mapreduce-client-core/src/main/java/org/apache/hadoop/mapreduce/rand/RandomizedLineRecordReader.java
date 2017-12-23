/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.rand;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.zookeeper.ZooKeeper;

public class RandomizedLineRecordReader extends LineRecordReader {
	private static final Log LOG = LogFactory.getLog(RandomizedLineRecordReader.class);

	// Configurations
	private RandomSamplingUtil util;

	// Parameters
	private long lines = 0;
	private long sampled = 0;
	private double ratio = 0;

	// random generator
	private Random rnd;

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
		super.initialize(genericSplit, context);
		rnd = new Random();
		util = RandomSamplingUtil.get(context.getConfiguration());
		ratio = util.getSamplingRatio();
	}

	@Override
	public boolean nextKeyValue() throws IOException {
		if (getFilePosition() > end)
			return false;
		if (key == null) {
			key = new LongWritable();
		}
		key.set(pos);
		if (value == null) {
			value = new Text();
		}

		while (true) {
			int size = in.readLine(value, maxLineLength, Math.max(maxBytesToConsume(pos), maxLineLength));
			if (size == 0) {
				key = null;
				value = null;
				return false;
			}
			lines++;
			pos += size;
			// equally pick every record
			if (rnd.nextDouble() < ratio) {
				sampled++;
				break;
			}
		}
		return true;
	}

	@Override
	public synchronized void close() throws IOException {
		ZooKeeper zk = util.connectZK();
		if (zk != null) {
			int count = util.updateCountZK(zk);
			util.writeDataZK(zk, count, lines, sampled);
		}
		util.closeZK(zk);
		super.close();
	}
}