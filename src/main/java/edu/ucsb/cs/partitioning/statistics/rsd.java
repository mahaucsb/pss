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
 * @Since Nov 6, 2012
 */

package edu.ucsb.cs.partitioning.statistics;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;

import edu.ucsb.cs.types.FeatureWeightArrayWritable;

/**
 * @author Maha Computes the Relative Standard Deviation for a binary input
 *         stored in a directory with types
 *         [longWritable,FeatureWeightArrayWritable].
 */
public class rsd {

	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.out
					.println("Usage:<input directory of (longWritable,FeatureWeightArrayWritable)> <p-norm>");
			return;
		}
		Configuration conf = new Configuration();
		Path inputPath = new Path(args[0]);
		FileSystem hdfs = inputPath.getFileSystem(conf);
		int lineCount = 0, pnorm = Integer.parseInt(args[1]);
		ArrayList<Float> pnorms = null, norm1 = null;
		Reader reader = null;

		if (!hdfs.exists(inputPath) || hdfs.isFile(inputPath)) {
			System.out.println("\n Input doesn't exists or is not a directory!");
			return;
		}

		FileStatus[] files = setFiles(hdfs, inputPath);
		for (int i = 0; i < files.length; i++) {
			inputPath = files[i].getPath();
			if (hdfs.isDirectory(inputPath) || inputPath.getName().startsWith("_"))
				continue;
			System.out.println("Reading file " + inputPath.getName()); // remove
			reader = new SequenceFile.Reader(hdfs, inputPath, conf);

			LongWritable key = new LongWritable();
			FeatureWeightArrayWritable value = new FeatureWeightArrayWritable();

			pnorms = new ArrayList<Float>();

			while (reader.next(key, value)) {
				pnorms.add(value.getPNorm(pnorm));
				lineCount++;
			}
		}
		float pnormrstd = getRStd(pnorms);

		System.out.println("\nInput has " + lineCount + " records.\n" + pnorm + "-Norm %-RSD = "
				+ (pnormrstd * 100));
		reader.close();
	}

	public static float getAvg(ArrayList<Float> array) {
		double sum = 0;
		for (int i = 0; i < array.size(); i++)
			sum += array.get(i);
		return (float) (sum / array.size());
	}

	public static float getRStd(ArrayList<Float> array) {
		float avg = getAvg(array);
		System.out.println("Avg. = " + avg);
		double sum = 0;
		for (int i = 0; i < array.size(); i++)
			sum += Math.pow(array.get(i) - avg, 2);
		double sd = Math.sqrt(sum / (array.size() - 1));
		return (float) (sd / avg);
	}

	public static FileStatus[] setFiles(FileSystem hdfs, Path inputPath) throws IOException {
		if (hdfs.isFile(inputPath))
			return hdfs.listStatus(inputPath.getParent());
		else
			return hdfs.listStatus(inputPath);
	}
}
