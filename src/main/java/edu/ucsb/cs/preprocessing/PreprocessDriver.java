package edu.ucsb.cs.preprocessing;

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

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ProgramDriver;

import edu.ucsb.cs.preprocessing.cleaning.WarcFilesDriver;
import edu.ucsb.cs.preprocessing.hashing.HashPagesDriver;
import edu.ucsb.cs.preprocessing.sequence.CombineSeqFiles;
import edu.ucsb.cs.preprocessing.sequence.SeqDriver;
import edu.ucsb.cs.preprocessing.sequence.SeqReader;

/**
 * Starts data preprocessing including: html cleaning, hashing, converting to
 * Hadoop sequence and reading the sequence files.
 */
public class PreprocessDriver {

	// private static final Logger LOG =
	// Logger.getLogger(BuildIntPostingsForwardIndex.class);
	// LOG.info("Input file: " + file);
	/**
	 * Prints these options to chose from:<br>
	 * - [Clean] html pages to produce bag of cleaned words. <br>
	 * - [Hash] bag of words into bag of hashed tokens.<br>
	 * - Produce [sequence] records [LongWritable,FeatureWeightArrayWritable] <br>
	 * - [Read] sequence records.
	 * 
	 * @param argv : command line inputs
	 */
	public static void main(String argv[]) {
		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();
		try {
			pgd.addClass("cleanhtml", WarcFilesDriver.class,
					"A MapReduce job to clean .warc.gz webpages from html and weird characters into set of features.");
			pgd.addClass(
					"hashrecords",
					HashPagesDriver.class,
					"A MapReduce job to collect features then hash input data into [docId <features>] with associated weights if desired. ");
			pgd.addClass("readseq", SeqReader.class,
					"Read sequence pages of the format [id<feature,weight>].");
			pgd.addClass("seqerecords", SeqDriver.class,
					"A MapReduce job to convert pages of the format [id<feature,weight>] into sequence files.");
			pgd.addClass("combineseqrec", CombineSeqFiles.class,
					"A regular java program to combine sequence records from multiple files into one file in hdfs.");
			pgd.driver(argv);
		} catch (Throwable e) {
			e.printStackTrace();
		}

		System.exit(exitCode);
	}

	public static void run(JobConf job) throws IOException {

		String ret = stars() + "\n  Running job:  " + job.getJobName() + "\n  Input Path:   {";
		Path inputs[] = FileInputFormat.getInputPaths(job);
		for (int ctr = 0; ctr < inputs.length; ctr++) {
			if (ctr > 0) {
				ret += "\n                ";
			}
			ret += inputs[ctr].toString();
		}
		ret += "}\n";
		ret += "  Output Path:  " + FileOutputFormat.getOutputPath(job) + "\n";
		System.err.println(ret);

		Date startTime = new Date();
		JobClient.runJob(job);
		Date end_time = new Date();
		System.err.println("Job took " + (end_time.getTime() - startTime.getTime())
				/ (float) 1000.0 + " seconds.");
	}

	public static String stars() {
		return new String(new char[77]).replace("\0", "*");
	}
}
