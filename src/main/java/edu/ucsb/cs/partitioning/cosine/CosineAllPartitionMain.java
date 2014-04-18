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
 * @Since Jul 14, 2012
 */

package edu.ucsb.cs.partitioning.cosine;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.ucsb.cs.hybrid.Config;
import edu.ucsb.cs.partitioning.PartDriver;
import edu.ucsb.cs.utilities.JobSubmitter;

/**
 * This class is responsible for producing cosine static partitions as a
 * filtering step before computing cosine similarity among documents.
 */
public class CosineAllPartitionMain extends CosinePartitioning {

	/**
	 * Executes four jobs. Job1 does norm-sorter. Job2 is a regular java program
	 * to uniformly partitions the records. Job3 runs the cosine partitioner and
	 * finally Job4 is the organizer to rename files into Gij and remove
	 * unnecessary partitioning through merging.
	 */
	public static void main(String[] args) throws IOException {
		runSort(args, "lengthsort");
		JobConf job = runUniformPartition(args, 3);
		runCosinePartition(job, args);
		rewritePartitions(job);
	}

	/**
	 * Job3: Core Cosine partitioning with skipping based on partition maximum
	 * vectors length, size and weight.
	 */
	public static JobConf runCosinePartition(JobConf job, String[] args) throws IOException {
		new GenericOptionsParser(job, args);
		job.setJobName(Partitioner.class.getSimpleName() + " + "
				+ CosineAllPartitionMain.class.getSimpleName());
		job.setJarByClass(CosineAllPartitionMain.class);
		job = setMapReduce(job, CosineAllPartMapper.class, IdentityReducer.class);
		job = setInputOutput(job, new Path(Partitioner.OUTPUT_DIR), interPath);
		JobSubmitter.run(job,"Cosine Partitioning",job.getFloat(Config.THRESHOLD_PROPERTY, Config.THRESHOLD_VALUE));
		FileSystem.get(job).delete(new Path(Partitioner.OUTPUT_DIR), true);
		return job;
	}
}