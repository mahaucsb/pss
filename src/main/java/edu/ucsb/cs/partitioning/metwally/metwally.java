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
 * @Since Apr 22, 2013
 */

package edu.ucsb.cs.partitioning.metwally;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;

import edu.ucsb.cs.hybrid.types.FeatureWeight;
import edu.ucsb.cs.hybrid.types.FeatureWeightArrayWritable;

/**
 * @author Maha Check if vectors [max(w_i), sqrt(1 - max(w_i)^2)] is still
 *         effective (ie. plotted into different partitions).
 */
public class metwally {

	static int s;
	static float threshold;
	static long pairCount = 0;

	public static void main(String[] args) {
		if (args.length != 3) {
			System.out.println("Usage: <seq_input_dir>  <metwally_s, s=-1 original> <threshold>");
			System.exit(3);
		}
		read(args);
		long t = System.currentTimeMillis();
		sim();

		System.out.println("# of ouput similari pairs = " + pairCount);
		System.out.println("time to calculate = " + (t - System.currentTimeMillis()));
	}

	public static ArrayList<FeatureWeightArrayWritable> block = new ArrayList<FeatureWeightArrayWritable>();

	public static void sim() {
		for (int i = 0; i < block.size() - 1; i++)
			for (int j = i + 1; j < block.size(); j++)
				mul2(block.get(i), block.get(j));
	}

	public static void read(String[] args) {

		try {
			Configuration conf = new Configuration();
			Path inputPath = new Path(args[0]);
			FileSystem fs = inputPath.getFileSystem(conf);
			System.out.println("Input path is: " + inputPath.toString());
			FileStatus[] files = fs.listStatus(inputPath);

			s = Integer.parseInt(args[1]);
			threshold = Float.parseFloat(args[2]);

			for (int i = 0; i < files.length; i++) {
				if (files[i].getPath().toString().startsWith("_"))
					continue;
				System.out.println("Reading file:" + files[i].getPath().toString());
				SequenceFile.Reader reader = new SequenceFile.Reader(fs, files[i].getPath(), conf);
				// SequenceFile.Writer writer = SequenceFile.createWriter(fs,
				// conf, new Path(args[1]
				// + "/" + i), LongWritable.class,
				// FeatureWeightArrayWritable.class,
				// SequenceFile.CompressionType.NONE);

				LongWritable key = new LongWritable();
				FeatureWeightArrayWritable value = new FeatureWeightArrayWritable();

				int k = -1;
				while (reader.next(key, value))
					if (value != null)
						block.add(new FeatureWeightArrayWritable(value.vectorSize, value.vector));
				reader.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void mul1(FeatureWeightArrayWritable v1, FeatureWeightArrayWritable v2) {

		int i = 0, j = 0, a = 0, b = 0;
		float product = 0;
		FeatureWeight[] vec1 = v1.sortById();
		FeatureWeight[] vec2 = v2.sortById();

		if (s == -1) {
			a = vec1.length - 1;
			b = vec2.length - 1;
		} else {
			a = s;
			b = s;
			if (s >= vec1.length) {
				a = vec1.length - 1;
			}
			if (s >= vec2.length) {
				b = vec2.length - 1;
			}
		}
		while (i <= a && j <= b)
			if (vec1[i].feature < vec2[j].feature)
				i++;
			else if (vec1[i].feature > vec2[j].feature)
				j++;
			else {
				product += (vec1[i].weight * vec2[j].weight);
				i++;
				j++;
			}
		float res1 = 0, res2 = 0;
		while (i < vec1.length) {
			res1 += vec1[i].weight;
			i++;
		}
		while (j < vec2.length) {
			res2 += vec2[j].weight;
			j++;
		}

		if (s == -1 && product >= threshold)
			pairCount++;
		else if ((product + res1 * res2) >= threshold)
			pairCount++;
	}

	public static void mul2(FeatureWeightArrayWritable v1, FeatureWeightArrayWritable v2) {
		int i = 0, j = 0, a = 0, b = 0;
		float product = 0;
		FeatureWeight[] vec1 = v1.sortById();
		FeatureWeight[] vec2 = v2.sortById();

		if (s == -1) {
			mul1(v1, v2);
			return;
		} else {
			a = s;
			b = s;
			if (s >= vec1.length) {
				a = vec1.length - 1;
			}
			if (s >= vec2.length) {
				b = vec2.length - 1;
			}
		}
		while (i <= a && j <= b) {
			product += (vec1[i].weight * vec2[j].weight);
			i++;
			j++;
		}
		float res1 = 0, res2 = 0;
		while (i < vec1.length) {
			res1 += vec1[i].weight;
			i++;
		}
		while (j < vec2.length) {
			res2 += vec2[j].weight;
			j++;
		}

		if (s == -1 && product >= threshold)
			pairCount++;
		else if ((product + res1 * res2) >= threshold)
			pairCount++;
	}
}