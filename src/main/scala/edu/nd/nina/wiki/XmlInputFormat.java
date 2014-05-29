package edu.nd.nina.wiki;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.io.InputStream;

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
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.io.Closeables;

/**
 * Reads records that are delimited by a specific begin/end tag.
 */
public class XmlInputFormat extends TextInputFormat {

	
	private static final Logger log = LoggerFactory
			.getLogger(XmlInputFormat.class);

	public static final String START_TAG_KEY = "xmlinput.start";
	public static final String END_TAG_KEY = "xmlinput.end";

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) {
		try {
			return new XmlRecordReader((FileSplit) split,
					context.getConfiguration());
		} catch (IOException ioe) {
			log.warn("Error while creating XmlRecordReader", ioe);
			return null;
		}
	}

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		final CompressionCodec codec = new CompressionCodecFactory(
				context.getConfiguration()).getCodec(file);
		if (null == codec) {
			return true;
		}
		return codec instanceof SplittableCompressionCodec;
	}

	/**
	 * XMLRecordReader class to read through a given xml document to output xml
	 * blocks as records as specified by the start tag and end tag
	 * 
	 */
	public static class XmlRecordReader extends
			RecordReader<LongWritable, Text> {

		private final byte[] startTag;
		private final byte[] endTag;
		private long start;
		private long pos;
		private long end;
		private FSDataInputStream fileIn;
		private InputStream in;
		private final DataOutputBuffer buffer = new DataOutputBuffer();
		private LongWritable currentKey;
		private Text currentValue;
		private Seekable filePosition;

		private CompressionCodec codec;
		private Decompressor decompressor;
		private CompressionCodecFactory compressionCodecs = null;

		public XmlRecordReader(FileSplit split, Configuration conf)
				throws IOException {
			startTag = conf.get(START_TAG_KEY).getBytes(Charsets.UTF_8);
			endTag = conf.get(END_TAG_KEY).getBytes(Charsets.UTF_8);

		}

		private boolean isCompressedInput() {
			return (codec != null);
		}

		private boolean next(LongWritable key, Text value) throws IOException {
			if (pos < end && readUntilMatch(startTag, false)) {
				try {
					buffer.write(startTag);
					if (readUntilMatch(endTag, true)) {
						key.set(pos);
						value.set(buffer.getData(), 0, buffer.getLength());
						return true;
					}
				} finally {
					buffer.reset();
				}
			}
			return false;
		}

		public synchronized void close() throws IOException {
			try {
				if (in != null) {
					in.close();
				}
			} finally {
				if (decompressor != null) {
					CodecPool.returnDecompressor(decompressor);
				}
			}
		}

		/**
		 * Get the progress within the split
		 */
		public float getProgress() throws IOException {
			if (start == end) {
				return 0.0f;
			} else {
				return Math.min(1.0f, (getFilePosition() - start)
						/ (float) (end - start));
			}
		}

		private long getFilePosition() throws IOException {
			long retVal;
			if (isCompressedInput() && null != filePosition) {
				retVal = filePosition.getPos();
			} else {
				retVal = pos;
			}
			return retVal;
		}

		
		private boolean readUntilMatch(byte[] match, boolean withinBlock)
				throws IOException {
			int i = 0;
			while (true) {
				int b = in.read();
				pos++;
				// end of file:
				if (b == -1) {
					return false;
				}
				// save to buffer:
				if (withinBlock) {
					buffer.write(b);
				}

				// check if we're matching:
				if (b == match[i]) {
					i++;
					if (i >= match.length) {
						return true;
					}
				} else {
					i = 0;
				}
				// see if we've passed the stop point:
				if (!withinBlock && i == 0 && pos >= end) {
					return false;
				}
			}
		}

		@Override
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			return currentKey;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return currentValue;
		}

		@Override
		public void initialize(InputSplit genericSplit,
				TaskAttemptContext context) throws IOException,
				InterruptedException {
			FileSplit split = (FileSplit) genericSplit;
			Configuration job = context.getConfiguration();

			start = split.getStart();
			end = start + split.getLength();
			final Path file = split.getPath();
			compressionCodecs = new CompressionCodecFactory(job);
			codec = compressionCodecs.getCodec(file);

			// open the file and seek to the start of the split
			final FileSystem fs = file.getFileSystem(job);
			fileIn = fs.open(file);
			if (isCompressedInput()) {
				decompressor = CodecPool.getDecompressor(codec);
				if (codec instanceof SplittableCompressionCodec) {
					final SplitCompressionInputStream cIn = ((SplittableCompressionCodec) codec)
							.createInputStream(
									fileIn,
									decompressor,
									start,
									end,
									SplittableCompressionCodec.READ_MODE.BYBLOCK);

					in = cIn;

					start = cIn.getAdjustedStart();
					end = cIn.getAdjustedEnd();
					filePosition = cIn;
				} else {

					in = codec.createInputStream(fileIn,
							decompressor);

					filePosition = fileIn;
				}
			} else {
				fileIn.seek(start);

				in = fileIn;

				filePosition = fileIn;
			}
			// If this is not the first split, we always throw away first record
			// because we always (except the last split) read one extra line in
			// next() method.
			
			this.pos = start;
		}

		private int maxBytesToConsume(long pos) {
			return isCompressedInput() ? Integer.MAX_VALUE : (int) Math.min(
					Integer.MAX_VALUE, end - pos);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			currentKey = new LongWritable();
			currentValue = new Text();
			return next(currentKey, currentValue);
		}
	}
}