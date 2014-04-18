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
 * @Since Sep 3, 2012
 */

package edu.ucsb.cs.lsh.minhash;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.ucsb.cs.hybrid.Config;
import edu.ucsb.cs.lsh.LshDriver;
import edu.ucsb.cs.lsh.types.IntArrayWritable;
import edu.ucsb.cs.utilities.JobSubmitter;

/**
 * This class is responsible for filtering out dissimilar jaccard records by
 * placing documents' signatures into the same buckets when they are likely to
 * be similar and to different buckets whenever they're likely not. It's a
 * mapReduce job with {@link LshMapper} as a class to generate the documents'
 * signatures and {@link LshReducer} to write documents sharing signatures into
 * the same file for all-to-all comparison later.
 * 
 * @author Maha Alabduljalil
 * 
 */
public class MinHashLshDriver {

	public static final String NAMESPACE = "lsh";
	public static final String THRESHOLD_PROPERTY = NAMESPACE + ".sim.threshold";
	public static final float THRESHOLD_VALUE = 0.44f;
	public static final String L_PROPERTY = NAMESPACE + ".l";
	public static final int L_VALUE = 32;
	public static final String K_PROPERTY = NAMESPACE + ".k";
	public static final int K_VALUE = 6;
	public static final String NUM_FEATURES_PROPERTY = NAMESPACE + ".num.features";
	public static final long NUM_FEATURES_VALUE = 6000;
	public static final String NUM_REDUCERS_PROPERTY = NAMESPACE + ".num.reducers";
	public static final int NUM_REDUCERS_VALUE = 3;

	public static void writeLsh(JobConf job, FileSystem fs, LshTable lshTable) {
		try {
			Path lshfile = new Path("lshfile");
			NullWritable none = NullWritable.get();
			if (fs.exists(lshfile))
				fs.delete(lshfile);
			SequenceFile.Writer writer = SequenceFile.createWriter(fs, job, lshfile,
					LshTable.class, NullWritable.class, SequenceFile.CompressionType.NONE);
			writer.append(lshTable, none);
			writer.close();
			DistributedCache.addCacheFile(new URI("lshfile"), job);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String args[]) throws ParseException, IOException {

		JobConf job = new JobConf();
		job.setJarByClass(MinHashLshDriver.class);
		job.setJobName(MinHashLshDriver.class.getSimpleName());
		GenericOptionsParser gop = new GenericOptionsParser(job, args);
		args = gop.getRemainingArgs();

		job.setMapperClass(LshMapper.class);
		job.setMapOutputKeyClass(IntArrayWritable.class); // signatures
		job.setMapOutputValueClass(LongWritable.class); // doc IDs
		job.setNumReduceTasks(job.getInt(NUM_REDUCERS_PROPERTY, NUM_REDUCERS_VALUE));
		job.setReducerClass(LshReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		String inputDir = args[0];
		if (inputDir == null) {
			throw new UnsupportedOperationException("ERROR: input directory not set.");
		}
		FileInputFormat.addInputPath(job, new Path(inputDir));
		Path outputPath = new Path("lsh-jaccard-buckets");
		FileOutputFormat.setOutputPath(job, outputPath);
		FileSystem.get(job).delete(outputPath, true);

		LshTable lshTable = new LshTable(job.getInt(K_PROPERTY, K_VALUE), job.getInt(L_PROPERTY,
				L_VALUE), 1024, job.getLong(NUM_FEATURES_PROPERTY, NUM_FEATURES_VALUE),
				job.getFloat(THRESHOLD_PROPERTY, THRESHOLD_VALUE));

		writeLsh(job, outputPath.getFileSystem(job), lshTable);

		JobSubmitter.run(job, "LSH", job.getFloat(THRESHOLD_PROPERTY, THRESHOLD_VALUE));
	}
}
