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
 * @Since Jul 13, 2012
 */

package edu.ucsb.cs.partitioning;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Date;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ProgramDriver;

import edu.ucsb.cs.partitioning.cosine.CosineAllPartitionMain;
import edu.ucsb.cs.partitioning.cosine.HolderCosinePartitionMain;
import edu.ucsb.cs.partitioning.jaccard.JaccardCoarsePartitionMain;
import edu.ucsb.cs.partitioning.metwally.metwally;
import edu.ucsb.cs.sort.SortDriver;

/**
 * The class input is a sequence input with one record per line processed by
 * Clueweb project with:<br>
 * KEY:LongWritable as id and VALUE: FeatureWeightArrayWritable to be the set of
 * features.
 */
public class PartDriver {

	public static final String NAMESPACE = "partition";
	public static final String THRESHOLD_PROPERTY = NAMESPACE + ".similarity.threshold";
	public static final float THRESHOLD_VALUE = 0.7f;
	public static final String NUM_PARTITIONS_PROPERTY = NAMESPACE + ".number.partitions";
	public static final int NUM_PARTITIONS_VALUE = 5;
	public static final String UNIFORM_PARTITIONING_PROPERTY = NAMESPACE + ".uniform.partitions";
	public static final Boolean UNIFORM_PARTITIONING_VALUE = false;
	public static final String PRINT_DISTRIBUTION_PROPERTY = NAMESPACE + ".print.distribution";
	public static final Boolean PRINT_DISTRIBUTION_VALUE = false;
	public static String INPUT_DIR = SortDriver.OUTPUT_DIR;
	public static String OUTPUT_DIR = "staticpartitions";

	public static void main(String args[]) throws UnsupportedEncodingException {

		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();
		try {
			pgd.addClass("jpartition", JaccardCoarsePartitionMain.class,
					"\tJaccard static partitioning");
			pgd.addClass("cpartitionn", HolderCosinePartitionMain.class,
					"\tCosine  static partitioning on p-norm sorted documents");
			pgd.addClass("metwally", metwally.class,
					"\tCosine  static partitioning following Metwally approach.");
			// pgd.addClass("cpartitionw", CosineWeightPartitionMain.class,
			// "\tCosine static partitioning on weight sorted documents");
			pgd.addClass("cpartitiona", CosineAllPartitionMain.class,
					"\tCosine static partitioning on ALL sorted documents");
			setup(args);
			pgd.driver(args);
			exitCode = 0;
		} catch (Throwable e) {
			e.printStackTrace();
		}
		System.exit(exitCode);
	}

	public static void setup(String[] args) throws UnsupportedEncodingException {
		if (args.length != 4)
			throw new UnsupportedEncodingException(
					"Usage: <className> -conf <confgs> <Unique Symbol>");
		INPUT_DIR += args[3];
		OUTPUT_DIR += args[3];
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
		ret += "  Output Path:  " + FileOutputFormat.getOutputPath(job) + "\n" + "  Map Tasks:    "
				+ job.getNumMapTasks() + "\n" + "  Reduce Tasks: " + job.getNumReduceTasks()
				+ "\n  Threshold:    " + job.getFloat(THRESHOLD_PROPERTY, THRESHOLD_VALUE);
		System.err.println(ret);

		Date startTime = new Date();
		JobClient.runJob(job);
		Date end_time = new Date();
		System.err.println("The job took " + (end_time.getTime() - startTime.getTime())
				/ (float) 1000.0 + " seconds.");
	}

	public static String stars() {
		return new String(new char[77]).replace("\0", "*");
	}
}
