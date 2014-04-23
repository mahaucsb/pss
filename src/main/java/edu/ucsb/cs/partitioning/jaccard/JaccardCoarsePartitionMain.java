/**
 * Copyright 2012-2013 The Regents of the University of California
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
 */

package edu.ucsb.cs.partitioning.jaccard;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.ucsb.cs.partitioning.Config;
import edu.ucsb.cs.partitioning.PartDriver;
import edu.ucsb.cs.partitioning.cosine.CosinePartitioning;
import edu.ucsb.cs.partitioning.statistics.Collector;
import edu.ucsb.cs.sort.SortDriver;
import edu.ucsb.cs.types.FeatureWeightArrayWritable;
import edu.ucsb.cs.utilities.JobSubmitter;

/**
 * This class takes a length-based sorted input of records and produce "n"
 * partitions (where "n" is specified by user) containing records exclusively.
 * Plus, it writes out a file called "skipList" to indicate which partition
 * skips which.
 * 
 *  * This class takes a length-based sorted input of records and produce "n"
 * partitions depending on threshold set (ie. satisfy condition min-i/max-i <t).
 * Plus, it writes out a file called "skipList" to indicate which partition
 * skips which.
 */
public class JaccardCoarsePartitionMain extends CosinePartitioning{

	public static String JACCARD_SKIP_PARTITIONS="jaccard_skip_partitions";

	private static int[] minPratitionRepresentors;
	private static int[] maxPratitionRepresentors;
	public static HashMap<Integer, ArrayList<Integer>> skipList = new HashMap<Integer, ArrayList<Integer>>();

	public static void main(String[] args) throws IOException {

		runSort(args, "lengthsort"); 
		JobConf job = new JobConf();
		new GenericOptionsParser(job, args);
		job.setJobName(JaccardCoarsePartitionMain.class.getSimpleName());
		job.setJarByClass(JaccardCoarsePartitionMain.class);
		//
		// set input & output & threshold & numPartitions
		//
		String inputDir = PartDriver.INPUT_DIR; //String inputDir = SortDriver.OUTPUT_DIR;
		FileSystem.get(job).delete(new Path(PartDriver.OUTPUT_DIR), true);
		float threshold = job.getFloat(Config.THRESHOLD_PROPERTY, Config.THRESHOLD_VALUE);
		int nPartitions = job.getInt(Config.NUM_PARTITIONS_PROPERTY,
				Config.NUM_PARTITIONS_VALUE);
		//
		// run regular java program
		//
		System.out.println(JobSubmitter.stars()
				+ "\n  Running Sequential Job:  jaccard coarse 1D partitioning "
				+ "\n  Threshold:  " + threshold);

		FileSystem hdfs = produceStaticParitions(inputDir, PartDriver.OUTPUT_DIR, nPartitions);
		produceSkipList(true, threshold, nPartitions, hdfs,job);
		Collector.printJaccardStatistics(job, PartDriver.OUTPUT_DIR);
	}

	public static FileSystem produceStaticParitions(String inputDir, String outputDir,
			int nPartitions) {

		// Variables
		FileSystem hdfs = null;
		FileStatus[] files;
		Configuration conf = new Configuration();
		Path outputDirectory = new Path(outputDir);
		SequenceFile.Writer out = null;

		LongWritable prevK, key = new LongWritable();
		FeatureWeightArrayWritable prevV, value = new FeatureWeightArrayWritable();

		setRepresentors(nPartitions);

		try {
			Path inputPath = new Path(inputDir);
			hdfs = inputPath.getFileSystem(conf);

			long nDocuments = Collector.countDirVectors(hdfs, inputPath, new JobConf());
			if (nDocuments == 0)
				return null;
			double partitionSize = Math.ceil(nDocuments / (double) nPartitions);

			if (partitionSize == 1)
				System.err.println("  WARN: Numper of partitions = number of documents!!");

			files = setFiles(hdfs, inputPath);

			int documentNo = 0, partitionNo = 0;
			for (int i = 0; i < files.length; i++) {
				inputPath = files[i].getPath();
				if (inputPath.toString().contains("_"))
					continue;
				Reader in = new SequenceFile.Reader(hdfs, inputPath, new Configuration());

				prevK = key;
				prevV = value;

				while (in.next(key, value)) {
					documentNo++;

					if (isFirstDocument(documentNo, partitionSize)) {
						setMinRepresentor(partitionNo, value.vectorSize);
						out = openFile(hdfs, outputDirectory, partitionNo);
						partitionNo++;
					}
					out.append(key, value);

					if (isLastDocument(documentNo, partitionSize)) {
						setMaxRepresentor(partitionNo - 1, value.vectorSize);
						out.close();
					}
					prevK = key;
					prevV = value;
				}
				in.close();
				setMaxRepresentor(partitionNo - 1, prevV.vectorSize);
				closeFile(out);
				nPartitions = partitionNo - 1;
			}
			return hdfs;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return hdfs;
	}

	/**
	 * @param printList
	 *            : print skip list to console if true
	 * @param writeList
	 *            : write skip list to "skipfile" if true
	 */
	public static void produceSkipList(Boolean writeList, float threshold, int nPartitions,
			FileSystem hdfs,JobConf job) throws IOException {

		for (int i = 0; i < nPartitions - 1; i++) {
			for (int j = i + 1; j < nPartitions; j++) {
				if (maxPratitionRepresentors[i] / (float) minPratitionRepresentors[j] < threshold) {
					addToList(i, j, nPartitions);
					break;
				}
			}
		}
		if (writeList)
			writeSkipFile(hdfs, nPartitions,job);
	}

	/**
	 * @param key
	 *            : first partition number
	 * @param value
	 *            : start number of the partitions to skip comparison with key
	 * @param list
	 *            : storage of pair of partitions to skip comparison with each
	 *            other
	 */
	public static void addToList(int key, int value, int nPartitions) {
		addListPair(key, value);
		addListPair(value, key);
		if (key < value)
			for (int k = (value + 1); k < nPartitions; k++) {
				skipList.get(key).add(k);
				addListPair(k, key);
			}
	}

	/* * Supporting Functions * */

	public static void addListPair(int key, int value) {

		ArrayList<Integer> iSkipList;
		if (skipList.containsKey(key))
			skipList.get(key).add(value);
		else {
			(iSkipList = new ArrayList<Integer>()).add(value);
			skipList.put(key, iSkipList);
		}
	}

	public static void setRepresentors(int nPartitions) {
		minPratitionRepresentors = new int[nPartitions];
		maxPratitionRepresentors = new int[nPartitions];
	}

	public static void setMinRepresentor(int partitionNo, int num) {
		minPratitionRepresentors[partitionNo] = num;
	}

	public static void setMaxRepresentor(int partitionNo, int num) {
		maxPratitionRepresentors[partitionNo] = num;
	}

	public static Boolean isFirstDocument(int documentNo, double partitionSize) {
		if (partitionSize == 1)
			return true;
		return (documentNo % partitionSize == 1 ? true : false);
	}

	public static Boolean isLastDocument(int documentNo, double partitionSize) {
		return (documentNo % partitionSize == 0 ? true : false);
	}

	public static SequenceFile.Writer openFile(FileSystem hdfs, Path parent, int child)
			throws IOException {
		return SequenceFile.createWriter(hdfs, new Configuration(), new Path(parent + "/" + child),
				LongWritable.class, FeatureWeightArrayWritable.class,
				SequenceFile.CompressionType.NONE);
	}

	public static void closeFile(SequenceFile.Writer out) throws IOException {
		if (out != null)
			out.close();
	}

	public static void writeSkipFile(FileSystem hdfs, int nPartitions,JobConf job) throws IOException {
		if(hdfs.exists(new Path(JACCARD_SKIP_PARTITIONS)))
			hdfs.delete(new Path(JACCARD_SKIP_PARTITIONS));

		if(hdfs.exists(new Path(JACCARD_SKIP_PARTITIONS+".txt")))
			hdfs.delete(new Path(JACCARD_SKIP_PARTITIONS+".txt"));

		MapFile.Writer skipWriter;
		skipWriter = new MapFile.Writer(job, hdfs, JACCARD_SKIP_PARTITIONS, IntWritable.class,
				Text.class);
		
		BufferedWriter br=new BufferedWriter(new OutputStreamWriter(hdfs.create(new Path(JACCARD_SKIP_PARTITIONS+".txt"),true)));

		String list="";
		for (int i = 0; i < nPartitions; i++)
			if (skipList.containsKey(i)) {
				br.write("\n" + i + ":");
				int key = i;
				for (int j = 0; j < skipList.get(i).size(); j++){
					br.write(skipList.get(i).get(j) + " ");
					list+=skipList.get(i).get(j)+" ";
				}
				skipWriter.append(new IntWritable(key), new Text(list));
			}
		br.close();
		skipWriter.close();
	}

	public static FileStatus[] setFiles(FileSystem hdfs, Path inputPath) throws IOException {
		if (hdfs.isFile(inputPath))
			return hdfs.listStatus(inputPath.getParent());
		else
			return hdfs.listStatus(inputPath);
	}
}
