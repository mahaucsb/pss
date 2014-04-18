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

package edu.ucsb.cs.sort.maxw;

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
 * </p>
 */
public class MaxwSortMain {

	/**
	 * Main method sets the job configurations including the mapper and reducer
	 * classes to do the sorting.
	 */
	public static void main(String[] args) throws IOException {

		JobConf job = new JobConf();
		new GenericOptionsParser(job, args);
		// ToolRunner.printGenericCommandUsage(System.out);
		job.setJobName(MaxwSortMain.class.getSimpleName());
		job.setJarByClass(MaxwSortMain.class);
		job.setMapperClass(MaxwSortMapper.class);
		job.setMapOutputKeyClass(FloatWritable.class);
		job.setMapOutputValueClass(IdFeatureWeightArrayWritable.class);

		job.setPartitionerClass(MaxwRangePartitioner.class);

		job.setReducerClass(MaxwSortReducer.class);
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
		JobSubmitter.run(job,"Sort By infinity-Norm",-1);
	}
}