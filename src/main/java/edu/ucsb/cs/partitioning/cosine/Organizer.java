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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;

import edu.ucsb.cs.partitioning.Config;
import edu.ucsb.cs.partitioning.PartDriver;
import edu.ucsb.cs.partitioning.statistics.Collector;
import edu.ucsb.cs.sort.norm.NormSortMain;
import edu.ucsb.cs.types.FeatureWeightArrayWritable;
import edu.ucsb.cs.types.IdFeatureWeightArrayWritable;
import edu.ucsb.cs.types.IntIntWritable;

/**
 * Makes the final touch of removing the key: "i j" from the beginning of each
 * document (IntIntWritable,IdFreatureWeightArrayWritable) to prepare them as
 * input the the hybrid similarity comparison, so the output is:
 * 
 * <pre>
 * KEY: LongWritable - docId
 * VALUE: FeatureWeightArrayWritable - holding the features of the document.
 * </pre>
 * 
 * It also incorporate the combining of unnecessary partitioning and prints
 * statistics about the data.
 */
public class Organizer {

	private static IntIntWritable unused = new IntIntWritable();
	private static IdFeatureWeightArrayWritable document = new IdFeatureWeightArrayWritable();

	public static void main(Path input, String output, JobConf job) throws IOException {
		readCombineCopy(input, output, job); // remove comment
		Collector.printCosineStatistics(job, output);
	}

	public static boolean isCombined(int pr, int pc, int cr, int cc, ArrayList<String> list) {
		if (((pr == 0) && (pc == 0)) || (cr == (cc + 1)) || ((pr != pc) && (cr == cc)))
			return false;
		for (String file : list) {
			int or = getRow(file);
			int oc = getCol(file);
			if (((or > pr) && (oc == pr) && (or != oc)))
				return false;
		}
		return true;
	}

	public static int getCol(String fileName) {
		return (Integer.parseInt(fileName.substring(fileName.indexOf("_") + 1)));
	}

	public static int getRow(String fileName) {
		return Integer.parseInt(fileName.substring(1, fileName.indexOf("_")));
	}

	public static ArrayList<String> arrangeNames(FileStatus[] files) {
		ArrayList<String> partitions = new ArrayList<String>();

		for (int i = 0; i < files.length; i++)
			if (files[i].getPath().getName().contains("G"))
				partitions.add(files[i].getPath().getName());

		Comparator<String> colComparator = new Comparator<String>() {

			public int compare(String o1, String o2) {
				if (getCol(o1) < getCol(o2))
					return -1;
				else if (getCol(o1) > getCol(o2))
					return 1;
				else if (getRow(o1) < getRow(o2))
					return -1;
				else if (getRow(o1) > getRow(o2))
					return 1;
				else
					return 0;
			}
		};

		Collections.sort(partitions, colComparator);
		return partitions;
	}

	public static void readCombineCopy(Path input, String output, JobConf job) throws IOException {
		boolean printDist = job.getBoolean(Config.PRINT_DISTRIBUTION_PROPERTY,
				Config.PRINT_DISTRIBUTION_VALUE);
		BufferedWriter distout = null;
		SequenceFile.Writer out = null;
		if (printDist)
			distout = new BufferedWriter(new FileWriter("p-norm-distribution" + output));
		
		int pc = 0, pr = 0;
		float pChoice = job.getFloat(NormSortMain.P_NORM_PROPERTY, NormSortMain.P_NORM_VALUE);
		FileSystem hdfs = input.getFileSystem(new JobConf());
		FileStatus[] files = Partitioner.setFiles(hdfs, input);
		ArrayList<String> partitions = arrangeNames(files);

		for (int i = 0; i < partitions.size(); i++) {
			Path inputPath = new Path(input.toString() + "/" + partitions.get(i));
			if (hdfs.isDirectory(inputPath))
				continue;

			SequenceFile.Reader in = new SequenceFile.Reader(hdfs, inputPath, job);
			if (!isCombined(pr, pc, getRow(inputPath.getName()), getCol(inputPath.getName()),
					partitions)) {
				if (out != null)
					out.close();
				pr = getRow(inputPath.getName());
				pc = getCol(inputPath.getName());
				out = SequenceFile.createWriter(hdfs, job,
						new Path(output + "/" + inputPath.getName()), LongWritable.class,
						FeatureWeightArrayWritable.class, SequenceFile.CompressionType.NONE);
			}
			while (in.next(unused, document)) {
				out.append(new LongWritable(document.id), new FeatureWeightArrayWritable(
						document.vectorSize, document.vector));
				if (printDist)
					distout.write(document.getPNorm(pChoice) + " \n");
			}
			in.close();
		}
		if (out != null)
			out.close();
	}
}