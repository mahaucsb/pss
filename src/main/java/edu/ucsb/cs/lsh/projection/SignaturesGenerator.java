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
 * @Since Jan 1, 2013
 */

package edu.ucsb.cs.lsh.projection;

/**
 * @author Maha
 * A Hadoop task to compute signatures for document vectors slightly modified from Ivory package.
 * Output is <docId,signature>, see ComputeSignaturesRandom.
 */

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.ucsb.cs.bruteforce.ForwardMapper;
import edu.ucsb.cs.lsh.types.BitSignature;
import edu.ucsb.cs.types.FeatureWeightArrayWritable;
import edu.ucsb.cs.utilities.JobSubmitter;
import edu.ucsb.cs.utilities.Properties;
import edu.umd.cloud9.io.SequenceFileUtils;

/**
 * A job to convert input sequence vectors into signatures each of size "nBits"
 * which is the number of projections. Then write those which share at least
 * sigThreshold distance into the same file using reducers.See
 * ComputeSignaturesRandom.
 * @author Maha
 * 
 */
public class SignaturesGenerator {

	public static String INPUT_DIR = "seqvectors";
	public static String OUTPUT_DIR = "sigvectors";

	public static void main(String[] args) throws Exception {
		JobConf job = new JobConf(SignaturesGenerator.class);
		new GenericOptionsParser(job, args);
		job.setJobName(SignaturesGenerator.class.getSimpleName());
		int nBits = job.getInt(ProjectionLshDriver.LSH_NBITS_PROPERTY,
				ProjectionLshDriver.LSH_NBITS_VALUE);
		setParameters();
		FileSystem fs = FileSystem.get(job);
		prepareDistributedCache(job, fs, new Path(ProjectionsGenerator.OUTPUT_DIR));
		Path outputPath = new Path(OUTPUT_DIR);
		if (fs.exists(outputPath))
			fs.delete(outputPath);

		FileInputFormat.setInputPaths(job, INPUT_DIR);
		// Path(INPUT_DIR));
		FileOutputFormat.setOutputPath(job, outputPath);
		// FileOutputFormat.setCompressOutput(job, false);
		job.setInputFormat(SequenceFileInputFormat.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);

		job.set("mapred.child.java.opts", "-Xmx2048m");
		job.setInt("mapred.map.max.attempts", 10);
		job.setInt("mapred.reduce.max.attempts", 10);
		job.setInt("mapred.task.timeout", 6000000);

		job.setMapperClass(SigMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(BitSignature.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(BitSignature.class);

		JobSubmitter.run(job,"LSH",-1);
	}

	public static void prepareDistributedCache(JobConf job, FileSystem fs, Path path)
			throws URISyntaxException, IOException {
		FileStatus[] files = fs.listStatus(path);
		System.out.println("path to read from is: " + path.getName()); // remove
		for (FileStatus file : files)
			if (fs.isFile(file.getPath()) && !file.getPath().getName().startsWith("_"))
				DistributedCache.addCacheFile(file.getPath().toUri(), job);
	}

	public static void setParameters() {
		Properties.requiredParameters.add("  Random projections directory: "
				+ ProjectionsGenerator.OUTPUT_DIR);
	}

	/**
	 * Converts sequence hashed vectors (long:docid, int:feature float:weight)
	 * into signatures using LSH.
	 */
	public static class SigMapper extends MapReduceBase implements
			Mapper<LongWritable, FeatureWeightArrayWritable, LongWritable, BitSignature> {

		static Path[] localFiles;
		static List<Writable> projectionUnitVectors;
		static int nBits;
		BitSignature signature;

		@Override
		public void configure(JobConf job) {
			nBits = job.getInt(ProjectionLshDriver.LSH_NBITS_PROPERTY,
					ProjectionLshDriver.LSH_NBITS_VALUE);
			signature = new BitSignature(nBits);
			try {
				localFiles = DistributedCache.getLocalCacheFiles(job);
				System.out.println("projection files:" + localFiles[0]);
				projectionUnitVectors = SequenceFileUtils.readValues(localFiles[0],
						FileSystem.getLocal(job)); // return list<ValueWritable>
			} catch (Exception e) {
				throw new RuntimeException("Error reading random vectors!");
			}

			System.out.println("projectionUnitVectors has: " + projectionUnitVectors.size());// remove
			// If threshold other than [0,0,..0]
			if (localFiles.length > 1) {
				float[] dotProductThresholds = new float[nBits];
				// copy rest from ComputeSignaturesRandom
			}
		}

		public void map(LongWritable docId, FeatureWeightArrayWritable docVector,
				OutputCollector<LongWritable, BitSignature> output, Reporter reporter)
				throws IOException {

			RandomVector randomProjection;
			for (int i = 0; i < projectionUnitVectors.size(); i++) {
				randomProjection = (RandomVector) projectionUnitVectors.get(i);
				float dotProduct = ForwardMapper.projectionDot(docVector, randomProjection);
				boolean sign = (dotProduct >= 0);
				signature.set(i, sign);
			}
			signature.setId(docId.get());
			signature.setVector(docVector);

			output.collect(docId, signature);
		}
	}
}
