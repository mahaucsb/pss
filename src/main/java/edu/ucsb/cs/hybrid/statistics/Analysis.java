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
 * @Since Aug 20, 2012
 */

package edu.ucsb.cs.hybrid.statistics;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;

import edu.ucsb.cs.types.FeatureWeightArrayWritable;

/**
 * should be embedded as a configuration or function call
 * Functions to produce statistics from the contributed optimizations.
 * @author Maha
 * 
 */
public class Analysis {

	public float getPrunedPercentage(JobConf job, Path inputPath) throws IOException {
		FileSystem fs = inputPath.getFileSystem(job);
		return 10.4f;
	}

	public void printStartingIndexedFeatureBargraph() {

	}

	public void printStartingFeatureBargraph() {

	}

	public static float AvgBp(int b, String inputDir, boolean excludeLonely) throws IOException {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		SequenceFile.Reader reader = null;
		FileStatus[] inputPaths = hdfs.listStatus(new Path(inputDir));
		LongWritable key = new LongWritable();
		FeatureWeightArrayWritable value = new FeatureWeightArrayWritable();
		HashMap<Long, Integer> featureCount = new HashMap<Long, Integer>();
		int count = 0;

		for (int f = 0; f < inputPaths.length; f++) {
			if (inputPaths[f].isDir() || inputPaths[f].getPath().getName().startsWith("_"))
				continue;
			reader = new SequenceFile.Reader(hdfs, inputPaths[f].getPath(), conf);
			while ((reader.next(key, value)) && count < b) {
				for (int i = 0; i < value.vectorSize; i++)
					if (featureCount.containsKey(value.vector[i].feature))
						featureCount.put(value.vector[i].feature,
								featureCount.get(value.vector[i].feature) + 1);
					else
						featureCount.put(value.vector[i].feature, 1);
				count++;
			}
			if (count >= b)
				break;
		}
		Iterator<Long> features = featureCount.keySet().iterator();
		long sumPostingsize = 0;
		long fCount = 0;
		while (features.hasNext()) {
			long feature = features.next();
			if (excludeLonely) {
				if (featureCount.get(feature) != 1) {
					fCount++;
					sumPostingsize += featureCount.get(feature);
				}
			} else {
				fCount++;
				sumPostingsize += featureCount.get(feature);
			}
		}
		return (sumPostingsize / (float) fCount);
	}

	public static float maxBp(int b, String inputDir, boolean excludeLonely) throws IOException {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		SequenceFile.Reader reader = null;
		FileStatus[] inputPaths = hdfs.listStatus(new Path(inputDir));
		LongWritable key = new LongWritable();
		FeatureWeightArrayWritable value = new FeatureWeightArrayWritable();
		HashMap<Long, Integer> featureCount = new HashMap<Long, Integer>();
		int count = 0;

		for (int f = 0; f < inputPaths.length; f++) {
			if (inputPaths[f].isDir() || inputPaths[f].getPath().getName().startsWith("_"))
				continue;
			reader = new SequenceFile.Reader(hdfs, inputPaths[f].getPath(), conf);
			while ((reader.next(key, value)) && count < b) {
				for (int i = 0; i < value.vectorSize; i++)
					if (featureCount.containsKey(value.vector[i].feature))
						featureCount.put(value.vector[i].feature,
								featureCount.get(value.vector[i].feature) + 1);
					else
						featureCount.put(value.vector[i].feature, 1);
				count++;
			}
			if (count >= b)
				break;
		}
		Iterator<Long> features = featureCount.keySet().iterator();
		long sumPostingsize = 0;
		long maxPost = 0;
		while (features.hasNext()) {
			long post = featureCount.get(features.next());
			if (post > maxPost)
				maxPost = post;
		}
		return maxPost;
	}

	public static void main(String[] args) throws IOException {

		System.out.println("Usage: <choice:1=avgBp,2=maxBp,..> <SeqInputDir>");

		int[] list = { 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024 };
		if (args[0].equals("1")) {
			System.out.println("Compute average of the number of documents sharing feature. ");
			for (int i = 0; i < list.length; i++)
				System.out.println("Average shared feature per " + list[i] + " vectors: "
						+ AvgBp(list[i], args[1], false));
			System.out
					.println("Compute average of the number of documents sharing feature (excluding lonely features). ");
			for (int i = 0; i < list.length; i++)
				System.out.println("Average shared feature per " + list[i] + " vectors: "
						+ AvgBp(list[i], args[1], true));
		} else if (args[0].equals("2")) {
			System.out.println("Compute maximum number of documents sharing feature. ");
			for (int i = 0; i < list.length; i++)
				System.out.println("Max shared feature per " + list[i] + " vectors: "
						+ maxBp(list[i], args[1], false));

		} else
			;
	}
}
