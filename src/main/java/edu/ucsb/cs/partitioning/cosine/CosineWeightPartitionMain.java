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

import org.apache.hadoop.mapred.JobConf;

/**
 * This class is responsible for producing cosine static partitions as a
 * filtering step before computing cosine similarity among documents.
 */
public class CosineWeightPartitionMain extends CosinePartitioning {

	/**
	 * Executes four jobs. Job1 does norm-sorter. Job2 is a regular java program
	 * to uniformly partitions the records. Job3 runs the cosine partitioner and
	 * finally Job4 is the organizer to rename files into Gij and remove
	 * unnecessary partitioning through merging.
	 */
	public static void main(String[] args) throws IOException {
		runSort(args, "maxwsort");
		JobConf job = runUniformPartition(args, 2);
		runCosinePartition(job, args, CosineWeightPartitionMain.class, CosineWeightPartMapper.class);
		rewritePartitions(job);
	}
}