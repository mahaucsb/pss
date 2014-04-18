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
 * @Since Dec 27, 2012
 */

package edu.ucsb.cs.lsh.projection;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.log4j.Logger;

import edu.ucsb.cs.hybrid.Config;
import edu.ucsb.cs.utilities.JobSubmitter;
import edu.ucsb.cs.utilities.Properties;
import edu.umd.cloud9.io.array.ArrayListOfFloatsWritable;

/**
 * @author Maha
 * 
 *         Copied from Ivory. Generates random projections for Cosine similarity
 *         and store it in "projections" in HDFS. This class has a data
 *         structure to optimize space when storing an array of floats. Floats
 *         are quantized into byte values and stored as a BytesWritable. maxNorm
 *         is a maxNormalization factor required to re-compute float from byte.
 * 
 *         byte = quantize(float) = ((float/maxNorm)*128)==128 ? 127 :
 *         ((float/maxNorm) float = dequantize(byte) = ((byte/128)*maxNorm)
 */
public class ProjectionsGenerator {

	private static final Logger sLogger = Logger.getLogger(ProjectionLshDriver.class);
	protected static String INPUT_DIR = "tmp";
	public static String OUTPUT_DIR = "projections";

	public static void main(JobConf job) throws IOException {
		int nBits/*D*/, nFeatures/*K*/, nReducers;
		job.setJobName(ProjectionsGenerator.class.getSimpleName());
		FileSystem fs = FileSystem.get(job);

		nBits = job.getInt(ProjectionLshDriver.LSH_NBITS_PROPERTY,
				ProjectionLshDriver.LSH_NBITS_VALUE);
		nFeatures = readCollectionFeatureCount(fs, job);
		setParameters(nBits, nFeatures);
		nReducers = job.getInt(ProjectionLshDriver.LSH_NREDUCER_PROPERTY,
				ProjectionLshDriver.LSH_NREDUCER_VALUE);
		Path inputPath = new Path(INPUT_DIR);
		Path outputPath = new Path(OUTPUT_DIR);
		if (fs.exists(outputPath))
			fs.delete(outputPath, true);
		if (fs.exists(inputPath))
			fs.delete(inputPath, true);

		SequenceFile.Writer writer = SequenceFile.createWriter(fs, job,
				new Path(inputPath.toString() + "/file"), IntWritable.class, IntWritable.class);
		for (int i = 0; i < nReducers; i++)
			writer.append(new IntWritable(i), new IntWritable(i));
		writer.close();

		job.setInputFormat(SequenceFileInputFormat.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		FileOutputFormat.setCompressOutput(job, false);

		job.set("mapred.child.java.opts", "-Xmx2048m");
		job.setInt("mapred.map.max.attempts", 10);
		job.setInt("mapred.reduce.max.attempts", 10);

		job.setNumMapTasks(1);
		job.setNumReduceTasks(nReducers);

		job.setMapperClass(IdentityMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(ProjectionReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(RandomVector.class);

		JobSubmitter.run(job,"LSH",job.getFloat(Config.THRESHOLD_PROPERTY, Config.THRESHOLD_VALUE));
	}

	public static int readCollectionFeatureCount(FileSystem hdfs, JobConf job) throws IOException {
		Path nFeaturesPath = new Path(Properties.NUM_FEATURES_FILE);
		if (hdfs.exists(nFeaturesPath)) {
			BufferedReader br = new BufferedReader(new InputStreamReader(new DataInputStream(
					new FileInputStream(nFeaturesPath.toString()))));
			String line;
			if ((line = br.readLine()) != null)
				job.setInt(ProjectionLshDriver.LSH_NFEATURES_PROPERTY, Integer.parseInt(line));

		}
		return job.getInt(ProjectionLshDriver.LSH_NFEATURES_PROPERTY,
				ProjectionLshDriver.LSH_NFEATURES_VALUE);
	}

	public static void setParameters(int nBits, int nFeatures) {
		Properties.requiredParameters.add("  Number of projection vectors: " + nBits);
		Properties.requiredParameters.add("  Size of each projection vector: " + nFeatures);
	}

	public static class ProjectionReducer extends MapReduceBase implements
			Reducer<IntWritable, IntWritable, IntWritable, RandomVector> {

		int nBits, nFeatures;
		RandomVector randomVector;
		IntWritable uniqueKey = new IntWritable();

		@Override
		public void configure(JobConf conf) {
			nBits = conf.getInt(ProjectionLshDriver.LSH_NBITS_PROPERTY,
					ProjectionLshDriver.LSH_NBITS_VALUE);
			nFeatures = conf.getInt(ProjectionLshDriver.LSH_NFEATURES_PROPERTY,
					ProjectionLshDriver.LSH_NFEATURES_VALUE);
		}

		public void reduce(IntWritable key, Iterator<IntWritable> values,
				OutputCollector<IntWritable, RandomVector> output, Reporter reporter)
				throws IOException {
			for (int i = 0; i < nBits; i++) {
				int index = (nBits * key.get()) + i; // unique num
				randomVector = ProjectionsGenerator.generateUnitRandomVectorAsBytes(nFeatures);
				uniqueKey.set(index);
				output.collect(uniqueKey, randomVector);
			}
		}
	}

	public static RandomVector generateUnitRandomVectorAsBytes(int numBits) {
		double r, x, y;
		ArrayListOfFloatsWritable vector = new ArrayListOfFloatsWritable(numBits);
		vector.setSize(numBits);

		byte[] bytes = new byte[numBits];
		float max = Float.MIN_VALUE;
		float min = Float.MAX_VALUE;

		for (int i = 0; i < numBits; i++) {

			// find a uniform random point (x, y) inside unit circle ie. zero
			// mean, unit variance.
			// http://en.wikipedia.org/wiki/Box-Muller_transform
			do {
				x = 2.0 * Math.random() - 1.0;
				y = 2.0 * Math.random() - 1.0;
				r = x * x + y * y;
			} while (r > 1 || r == 0); // loop executed 4 / pi = 1.273.. times
			// on average

			// apply the Box-Muller formula to get standard Gaussian z
			float f = (float) (x * Math.sqrt(-2.0 * Math.log(r) / r));
			vector.set(i, f);
			if (f > 0 && f > max) {
				max = f;
			} else if (f < 0 && f < min) {
				min = f;
			}

		}

		/* normalize vector */
		for (int i = 0; i < vector.size(); i++) {
			float val = vector.get(i);
			float normalized2one = 0.0f;
			// map values to [-1,1] range
			if (val > 0) {
				normalized2one = val / max;
			} else if (val < 0) {
				normalized2one = val / Math.abs(min);
			}
			// quantize float to byte
			int byteInt = (int) (normalized2one * (Byte.MAX_VALUE + 1));

			byte b;
			if (byteInt > Byte.MAX_VALUE) {
				b = Byte.MAX_VALUE;
			} else {
				b = (byte) byteInt;
			}

			bytes[i] = b;
		}
		RandomVector vector2 = new RandomVector(bytes, max, min);
		return vector2;
	}
}
