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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ProgramDriver;

import edu.ucsb.cs.partitioning.cosine.CosineAllPartitionMain;
import edu.ucsb.cs.partitioning.cosine.HolderCosinePartitionMain;
import edu.ucsb.cs.partitioning.jaccard.JaccardCoarsePartitionMain;
import edu.ucsb.cs.sort.SortDriver;

/**
 * The class input is a sequence input with one record per line processed by
 * Clueweb project with:<br>
 * KEY:LongWritable as id and VALUE: FeatureWeightArrayWritable to be the set of
 * features.
 */
public class PartDriver {

	public static String INPUT_DIR = SortDriver.OUTPUT_DIR;
	public static String OUTPUT_DIR = "staticpartitions";

	public static void main(String args[]) throws UnsupportedEncodingException {

		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();
		try {
			JobConf job = new JobConf();
			new GenericOptionsParser(job, args);
			String metric = job.get(Config.METRIC_PROPERTY,Config.METRIC_VALUE).toLowerCase();
			if(metric.contains("j")){
				JaccardCoarsePartitionMain.main(args);
			}else 
				HolderCosinePartitionMain.main(args);
			//			// pgd.addClass("cpartitionw", CosineWeightPartitionMain.class,
			//			// "\tCosine static partitioning on weight sorted documents");
			//			pgd.addClass("cpartitiona", CosineAllPartitionMain.class,
			//					"\tCosine static partitioning on ALL sorted documents");
			//pgd.driver(args);
			exitCode = 0;
		} catch (Throwable e) {
			e.printStackTrace();
		}
		System.exit(exitCode);
	}
}
