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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;

import edu.ucsb.cs.types.FeatureWeightArrayWritable;

/**
 * Given a binary input of the format: longWritable,FreqWeightArrayWritable.
 * This code produces a histogram where x-axis are the interval of the p-values
 * and y-axis is the frequency.
 * @author Maha
 */
public class DistributionPlotter {

	public static HashMap<Integer, Integer> historgram = new HashMap<Integer, Integer>();
	public static String input;
	public static String output;
	public static int max = 0;
	public static float p = 1, range = 1.0f;

	public static void main(String[] args) throws IOException {

		if (args.length != 4)
			printUsage();

		input = args[0];
		output = args[1];
		range = Float.parseFloat(args[2]);
		p = Float.parseFloat(args[3]);

		Configuration conf = new Configuration();
		Path inputPath = new Path(input);
		FileSystem hdfs = inputPath.getFileSystem(conf);
		int lineCount = 0;
		double avg = 0, variance = 0;
		ArrayList<Float> pnorms = new ArrayList<Float>();
		Reader reader = null;

		if ((!hdfs.exists(inputPath)) || (!hdfs.isDirectory(inputPath)))
			printUsage();

		FileStatus[] files = setFiles(hdfs, inputPath);
		for (int i = 0; i < files.length; i++) {
			inputPath = files[i].getPath();
			if (hdfs.isDirectory(inputPath) || inputPath.getName().startsWith("_"))
				continue;
			System.out.println("Reading file " + inputPath.getName()); // remove
			reader = new SequenceFile.Reader(hdfs, inputPath, conf);

			LongWritable key = new LongWritable();
			FeatureWeightArrayWritable value = new FeatureWeightArrayWritable();

			while (reader.next(key, value)) {
				float x = value.getPNorm(p);
				avg += x;
				pnorms.add(x);
				int pNorm = findRange(x);
				if (max < pNorm)
					max = pNorm;
				int bar = pNorm;
				if (historgram.containsKey(bar))
					historgram.put(bar, historgram.get(bar) + 1);
				else
					historgram.put(bar, 1);
				lineCount++;
			}
			reader.close();
		}
		avg /= lineCount;
		for (int i = 0; i < pnorms.size(); i++)
			variance += Math.pow(pnorms.get(i) - avg, 2);
		variance /= (lineCount - 1);
		writeHistorgramToFile(output, avg, variance);
		System.out.println(lineCount + " vectors are processed. ");
	}

	public static int findRange(float pnorm) {
		if ((0.99f <= pnorm && pnorm < 1.0f))
			return 1;
		return ((int) (pnorm / range));
	}

	public static void writeHistorgramToFile(String outfile, double avg, double var)
			throws IOException {
		BufferedWriter out = new BufferedWriter(new FileWriter(outfile));
		List<Integer> historgramByKeys = sortMapByKeys();

		for (int p = 0; p <= max; p++)
			if (!historgram.containsKey(p))
				out.write((p * range) + "\t" + 0 + "\n");
			else
				out.write((p * range) + "\t" + historgram.get(p) + "\n");
		out.write("Avg.: " + avg + "\n");
		out.write("Variance: " + var + "\n");
		out.close();
	}

	public static List<Integer> sortMapByKeys() {

		List<Integer> historgramByKeys = new ArrayList<Integer>(historgram.keySet());

		Collections.sort(historgramByKeys, new Comparator<Integer>() {

			public int compare(Integer o1, Integer o2) {
				return (o1 - o2);
			}
		});
		return historgramByKeys;
	}

	public static void printUsage() {
		System.out
				.println("Usage:<binaryDir:Long,FeatureWeigth> <output dummy> <historgram range size> <p for norm>");
		System.exit(0);
	}

	public static FileStatus[] setFiles(FileSystem hdfs, Path inputPath) throws IOException {
		if (hdfs.isFile(inputPath))
			return hdfs.listStatus(inputPath.getParent());
		else
			return hdfs.listStatus(inputPath);
	}
}
