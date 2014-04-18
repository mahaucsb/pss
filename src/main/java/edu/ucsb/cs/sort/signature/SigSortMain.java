/**
 * Copyright 2012-2013 The Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on
 * an "AS IS"; BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under
 * the License.
 * 
 * Author: maha alabduljalil <maha (at) cs.ucsb.edu>
 * @Since Jul 2012
 */

package edu.ucsb.cs.sort.signature;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.ucsb.cs.lsh.projection.SignaturesGenerator;
import edu.ucsb.cs.lsh.types.BitSignature;
import edu.ucsb.cs.sort.SortDriver;
import edu.ucsb.cs.utilities.JobSubmitter;

/**
 * This class sorts sequence file input of the types:
 * 
 * <pre>
 * KEY: LongWritable, VALUE: BitSignature
 * </pre>
 * 
 * by signature byte by byte.
 */
public class SigSortMain {

	public static String INPUT_PATH = SignaturesGenerator.OUTPUT_DIR;
	public static String OUTPUT_PATH = "sortedsigs";

	/**
	 * Sets the job configurations including the mapper and reducer classes to
	 * do the sorting based signatures.
	 */
	public static void main(String[] args) throws IOException {

		JobConf job = new JobConf();
		new GenericOptionsParser(job, args);
		job.setJobName(SigSortMain.class.getSimpleName());
		job.setJarByClass(SigSortMain.class);
		job.setMapperClass(SigSortMapper.class);
		job.setMapOutputKeyClass(BitSignature.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setPartitionerClass(SigRangePartitioner.class);

		job.setReducerClass(SigSortReducer.class);
		job.setNumReduceTasks(job.getInt(SortDriver.NUM_REDUCE_PROPERTY,
				SortDriver.NUM_REDUCE_VALUE));
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(BitSignature.class);
		//
		// set input & output
		//
		String inputDir = SortDriver.INPUT_DIR;
		if (inputDir == null) {
			throw new UnsupportedOperationException("ERROR: input path not set");
		}
		job.setInputFormat(SequenceFileInputFormat.class);
		SequenceFileInputFormat.addInputPath(job, new Path(inputDir));
		Path outputPath = new Path(OUTPUT_PATH);
		FileSystem.get(job).delete(outputPath, true);
		job.setOutputFormat(SequenceFileOutputFormat.class);
		FileOutputFormat.setOutputPath(job, outputPath);

		//
		// run
		//
		JobSubmitter.run(job,"Sort By Signature Bytes",-1);
	}
}