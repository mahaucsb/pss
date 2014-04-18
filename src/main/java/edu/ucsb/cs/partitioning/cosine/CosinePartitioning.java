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
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.MultipleSequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.ucsb.cs.hadoop.NonSplitableSequenceInputFormat;
import edu.ucsb.cs.partitioning.Config;
import edu.ucsb.cs.partitioning.PartDriver;
import edu.ucsb.cs.sort.SortDriver;
import edu.ucsb.cs.types.IdFeatureWeightArrayWritable;
import edu.ucsb.cs.types.IntIntWritable;
import edu.ucsb.cs.utilities.JobSubmitter;

/**
 * Abstract class for all classes that produce cosine static partitions as a
 * static filtering step before computing cosine similarity among documents. It
 * will generate partitions and assigned them names that imply which other
 * partition to skip.
 */
public abstract class CosinePartitioning {

	public static final Path interPath = new Path("partitions-Gij");

	/**
	 * Job1: Executes the sorting based p-norm choice provided.
	 */
	public static void runSort(String[] args, String sortChoice)
			throws UnsupportedEncodingException {
		SortDriver.runJob(sortChoice, args);
	}

	/**
	 * Job2: Performs uniform partitioning.
	 */
	public static JobConf runUniformPartition(String[] args, int partitionChoice_norm_weight_all)
			throws IOException {
		return Partitioner.main(args, partitionChoice_norm_weight_all);
	}

	/**
	 * Job3: Core Cosine partitioning on sorted weights vectors.
	 */
	public static void runCosinePartition(JobConf job, String[] args, Class jobSpawner, Class mapper)
			throws IOException {
		new GenericOptionsParser(job, args);
		job.setJobName(Partitioner.class.getSimpleName() + " + " + jobSpawner.getSimpleName());
		job = setMapReduce(job, mapper, IdentityReducer.class);
		job = setInputOutput(job, new Path(Partitioner.OUTPUT_DIR), interPath);
		JobSubmitter.run(job,"Cosine Partitioning",job.getFloat(Config.THRESHOLD_PROPERTY, Config.THRESHOLD_VALUE));
		// FileSystem.get(job).delete(new Path(Partitioner.OUTPUT_DIR), true);
	}

	/**
	 * Job4: Rewrites and merges unnecessary partitioning.
	 */
	public static void rewritePartitions(JobConf job) throws IOException {
		Path outputPath = new Path(PartDriver.OUTPUT_DIR);
		FileSystem.get(job).delete(outputPath, true);
		System.out.println(JobSubmitter.stars()
				+ "\n Running Organizer to remove unnecessary partitionins --> "
				+ outputPath.getName());
		Organizer.main(interPath, outputPath.getName(), job);
		FileSystem.get(job).delete(interPath, true);
	}

	/**
	 * Sets MapReduce input configurations for the core cosine partitioning job.
	 */
	public static JobConf setMapReduce(JobConf job, Class mapper, Class reducer) {
		job.setMapperClass(mapper);
		job.setMapOutputKeyClass(IntIntWritable.class);
		job.setMapOutputValueClass(IdFeatureWeightArrayWritable.class);
		job.setNumReduceTasks(job.getInt(Config.NUM_PARTITIONS_PROPERTY,
				Config.NUM_PARTITIONS_VALUE));
		job.setReducerClass(reducer);
		job.setOutputKeyClass(IntIntWritable.class);
		job.setOutputValueClass(IdFeatureWeightArrayWritable.class);
		return job;
	}

	public static JobConf setInputOutput(JobConf job, Path inputPath, Path outputPath)
			throws IOException {
		job.setInputFormat(NonSplitableSequenceInputFormat.class);
		SequenceFileInputFormat.addInputPath(job, inputPath);
		FileSystem.get(job).delete(outputPath, true);
		job.setOutputFormat(MultiSeqOutput.class);
		MultiSeqOutput.setOutputPath(job, outputPath);
		return job;
	}

	static class MultiSeqOutput extends
			MultipleSequenceFileOutputFormat<IntIntWritable, IdFeatureWeightArrayWritable> {
		@Override
		protected String generateFileNameForKeyValue(IntIntWritable key,
				IdFeatureWeightArrayWritable value, String name) {
			return ("G" + key.x + "_" + key.y);
		}
	}
}