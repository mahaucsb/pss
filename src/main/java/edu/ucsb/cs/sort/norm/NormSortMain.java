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

package edu.ucsb.cs.sort.norm;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.ucsb.cs.sort.SortDriver;
import edu.ucsb.cs.types.FeatureWeightArrayWritable;
import edu.ucsb.cs.types.IdFeatureWeightArrayWritable;
import edu.ucsb.cs.utilities.JobSubmitter;

/**
 * Produces partitions with sorted vectors based on their p-norm. The produced
 * partitions might be merged later to reflect the user choice. </p>
 */
public class NormSortMain {

	public static final String P_NORM_PROPERTY = "sort.p.norm";
	public static final float P_NORM_VALUE = 1.0f;

	/**
	 * Main method sets the job configurations including the mapper and reducer
	 * classes to do the sorting. Some of the produced partitions might be
	 * merged later to reflect the number of partitions chosen by the user.
	 */
	public static void main(String[] args) throws IOException {

		JobConf job = new JobConf();
		new GenericOptionsParser(job, args);
		job.setJobName("NormSort");
		job.setJarByClass(NormSortMain.class);
		job.setMapperClass(NormSortMapper.class);
		job.setMapOutputKeyClass(FloatWritable.class);
		job.setMapOutputValueClass(IdFeatureWeightArrayWritable.class);

		job.setPartitionerClass(NormRangePartitioner.class);

		job.setReducerClass(NormSortReducer.class);
		job.setNumReduceTasks(job.getInt(SortDriver.NUM_REDUCE_PROPERTY,
				SortDriver.NUM_REDUCE_VALUE));
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(FeatureWeightArrayWritable.class);
		//
		// set input & output
		//
		String inputDir = SortDriver.INPUT_DIR;
		if (inputDir == null) {
			throw new UnsupportedOperationException("ERROR: input path not set");
		}
		job.setInputFormat(SequenceFileInputFormat.class);
		SequenceFileInputFormat.addInputPath(job, new Path(inputDir));
		Path outputPath = new Path(SortDriver.OUTPUT_DIR);
		FileSystem.get(job).delete(outputPath, true);
		job.setOutputFormat(SequenceFileOutputFormat.class);
		FileOutputFormat.setOutputPath(job, outputPath);
		//
		// run
		//
		JobSubmitter.run(job,"Sort By p-norm",-1);
	}
}