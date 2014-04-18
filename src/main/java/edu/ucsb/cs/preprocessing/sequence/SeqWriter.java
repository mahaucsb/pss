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
 * @Since Jul 27, 2012
 */

package edu.ucsb.cs.preprocessing.sequence;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

import edu.ucsb.cs.preprocessing.PreprocessDriver;
import edu.ucsb.cs.preprocessing.hashing.HashPagesDriver;
import edu.ucsb.cs.types.FeatureWeightArrayWritable;
import edu.ucsb.cs.utilities.JobSubmitter;

public class SeqWriter {

	private static String INPUT_DIR = HashPagesDriver.OUTPUT_DIR;
	public static String OUTPUT_DIR = "seqvectors";

	public static void main(String[] args) throws IOException, ParseException {
		writeSequence();
	}

	/**
	 * Runs a MR job with maps only to convert input directory of numeric valued
	 * records to hadoop sequence format. It assumes a text input of format of
	 * [id feature weight ..] to be the format of input.
	 */
	public static void writeSequence() throws IOException {

		JobConf job = new JobConf();
		job.setJobName("Convert text vectors to hadoop seqeunce ");
		job.setJarByClass(SeqWriter.class);

		job.setMapperClass(SeqMapper.class);
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(FeatureWeightArrayWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(FeatureWeightArrayWritable.class);

		job.setInputFormat(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(INPUT_DIR));
		FileSystem.get(job).delete(new Path(HashPagesDriver.IDS_FILE2), true);
		Path outputPath = new Path(OUTPUT_DIR);
		FileSystem.get(job).delete(outputPath, true);
		job.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, outputPath);

		JobSubmitter.run(job,"PREPROCESS",-1);
	}
}