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

package edu.ucsb.cs.partitioning.cosine;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.ucsb.cs.partitioning.Config;
import edu.ucsb.cs.partitioning.PartDriver;
import edu.ucsb.cs.partitioning.statistics.Collector;
import edu.ucsb.cs.partitioning.statistics.GijComparisons;
import edu.ucsb.cs.sort.SortDriver;
import edu.ucsb.cs.sort.norm.NormSortMain;
import edu.ucsb.cs.types.FeatureWeightArrayWritable;
import edu.ucsb.cs.utilities.JobSubmitter;

/**
 * This class takes a norm/weight-based sorted input of records of the format:
 * 
 * <pre>
 * KEY: longWritable , VALUE: FeatureWeightArrayWritable
 * </pre>
 * 
 * and produce "n" partitions (uniform or nonUniform), where "n" is specified by
 * user starting with 1...n, containing records exclusively.<br>
 * It also outputs a file depending on the user choice as one of that follows:<br>
 * <br>
 * 1) <b>maxnorm</b>: max 1-norm for each of the output partitions.<br>
 * 2) <b>maxweight</b>: max weight for each of the output partitions. <br>
 * 3) <b>maxall</b>: max (norm , weight, vector size) of each of the output
 * partitions.<br>
 */
public class Partitioner {

	/** Uniform partitions. */
	public static final String OUTPUT_DIR = "partitions-Gi";
	/**
	 * Contains max (norm,weight,length) for each produced partitions to guide
	 * the static partitioning that follows.
	 */
	public static final String MAX_DIR_PATH = "max.dir.path";
	private static boolean uniformPartitions = true;

	/**
	 * Uniformly partitions the sequence vectors given the number of partitions
	 * input in the configuration file. It also prepares partitions information
	 * about its partitions: maximum p-norms, weights or norm/weights/lengths in
	 * a file to guide the core static partitioning next for skipping.
	 * 
	 * @param norm_weight_all
	 * @return
	 */
	public static JobConf main(String[] args, int norm_weight_all) throws IOException {

		JobConf job = new JobConf();
		new GenericOptionsParser(job, args);
		job.setJarByClass(Partitioner.class);
		System.out.println(JobSubmitter.stars()
				+ "\n Running partitioner to prepare uniform partitionins (Single JVM) ");

		String inputDir = SortDriver.OUTPUT_DIR, maxDir;

		if (norm_weight_all == 1)
			maxDir = "/maxpnorm";
		//		maxDir = inputDir + "/maxpnorm";
		else if (norm_weight_all == 2)
			maxDir = "/maxweight";
		//		maxDir = inputDir + "/maxweight";
		else
			maxDir = "/maxall";
		//		maxDir = inputDir + "/maxall";

		if (!(new Path(inputDir).getFileSystem(job)).exists(new Path(inputDir)))
			throw new UnsupportedOperationException("ERROR: " + inputDir + " directory not set.");

		job.set(MAX_DIR_PATH, maxDir);
		job.set(Config.NUM_PARTITIONS_PROPERTY, Integer.toString(produceStaticParitions(job,
				inputDir, OUTPUT_DIR, maxDir,
				job.getInt(Config.NUM_PARTITIONS_PROPERTY, Config.NUM_PARTITIONS_VALUE),
				norm_weight_all)));
		return job;
	}

	/**
	 * 
	 * @param job
	 * @param inputDir
	 * @param interDir
	 * @param maxDir
	 * @param nPartitions
	 * @param norm_weight_all
	 * @return number of partitions actaully produced
	 */
	public static int produceStaticParitions(JobConf job, String inputDir, String interDir,
			String maxDir, int nPartitions, int norm_weight_all) {
		SequenceFile.Writer partOut = null;
		float maxn = 0, maxw = 0, pChoice = job.getFloat(NormSortMain.P_NORM_PROPERTY,
				NormSortMain.P_NORM_VALUE);
		int maxs = 0;
		LongWritable prevK = null, key = new LongWritable();
		FeatureWeightArrayWritable prevV = null, value = new FeatureWeightArrayWritable();

		try {
			Path inputPath = new Path(inputDir);
			FileSystem hdfs = inputPath.getFileSystem(new Configuration());
			Path interDirectory = new Path(interDir);
			Path maxPath = new Path(maxDir);

			clearPath(hdfs, maxPath);
			clearPath(hdfs, interDirectory);

			long nDocuments = Collector.countDirVectors(hdfs, inputPath, job);
			if (nDocuments == 0)
				return 0;

			double partitionSize;
			uniformPartitions = job.getBoolean(Config.UNIFORM_PARTITIONING_PROPERTY,
					Config.UNIFORM_PARTITIONING_VALUE);
			if (uniformPartitions)
				partitionSize = Math.ceil(nDocuments / (double) nPartitions);
			else
				partitionSize = Math.ceil(nDocuments
						/ (double) (GijComparisons.choose(nPartitions + 1, 2)));

			if (partitionSize == 1)
				System.err.println("WARN: Number of partitions = number of documents!!");

			FileStatus[] files = setFiles(hdfs, inputPath);
			FSDataOutputStream maxOut = hdfs.create(maxPath);

			int documentNo = 0, partitionNo = 1; // partition naming start at 1
			for (int i = 0; i < files.length; i++) {
				inputPath = files[i].getPath();
				if ((hdfs.isDirectory(inputPath) || inputPath.getName().startsWith("_")))
					continue;
				Reader in = new SequenceFile.Reader(hdfs, inputPath, job);

				while (in.next(key, value)) { // id,vector
					documentNo++;
					prevK = key;
					prevV = value;

					if (isFirstDocument(partOut)) {
						maxn = value.getPNorm(pChoice);
						maxw = value.getMaxWeight();
						maxs = value.vectorSize;
						partOut = openFile(hdfs, job, interDirectory, partitionNo);
					}
					partOut.append(key, value);
					maxw = (value.getMaxWeight() > maxw) ? value.getMaxWeight() : maxw;
					maxs = (value.vectorSize > maxs) ? value.vectorSize : maxs;
					maxn = (value.getPNorm(pChoice) > maxn) ? value.getPNorm(pChoice) : maxn;

					if (isLastDocument(documentNo, partitionNo, partitionSize, uniformPartitions)) {
						partOut = writeMax(norm_weight_all, partOut, maxOut, maxn, maxw, maxs);
						documentNo = 0;
						partitionNo++;
					}
					prevK = key;
					prevV = value;
				}
				in.close();
			}
			if (partOut != null)
				partOut = writeMax(norm_weight_all, partOut, maxOut, maxn, maxw, maxs);
			nPartitions = partitionNo - 1;
			maxOut.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return (nPartitions);
	}

	public static void clearPath(FileSystem fs, Path path) throws IOException {
		if (fs.exists(path))
			fs.delete(path);
	}

	/**
	 * Writes the partitions leader information for static partitioning. Eg. max
	 * weight or max 1-norm or max vector size.
	 * @param norm_weight_all: choice to write to file.
	 * @param partOut: current fished partition written to close.
	 * @param maxOut: file of partitions information.
	 * @param max_pnorm: current partition max 1-norm.
	 * @param max_weight: current partition max weight.
	 * @param max_size: current partition max vector size.
	 * @return closed partition file handler for next reuse.
	 */
	public static SequenceFile.Writer writeMax(int norm_weight_all, SequenceFile.Writer partOut,
			FSDataOutputStream maxOut, float max_pnorm, float max_weight, int max_size)
					throws IOException {
		if (norm_weight_all == 1)
			maxOut.writeChars(max_pnorm + "\n");
		else if (norm_weight_all == 2)
			maxOut.writeChars(max_weight + "\n");
		else
			maxOut.writeChars(max_pnorm + "," + max_weight + "," + max_size + "\n");
		partOut.close();
		partOut = null;
		return partOut;
	}

	public static Boolean isFirstDocument(SequenceFile.Writer partOut) {
		return ((partOut == null) ? true : false);
	}

	public static Boolean isLastDocument(int documentNo, int currentPNum, double partitionSize,
			boolean uniformPartitions) {
		if (partitionSize == 1)
			return true;
		else if (!uniformPartitions) {
			return (documentNo % ((3 - currentPNum) * partitionSize) == 0 ? true : false);
		} else
			return (documentNo % partitionSize == 0 ? true : false);
	}

	public static SequenceFile.Writer openFile(FileSystem hdfs, JobConf job, Path parent, int child)
			throws IOException {
		Path outputPath = new Path(parent + "/" + child);
		SequenceFile.Writer out = SequenceFile.createWriter(hdfs, job, outputPath,
				LongWritable.class, FeatureWeightArrayWritable.class,
				SequenceFile.CompressionType.NONE);
		return out;
	}

	public static void closeFile(FSDataOutputStream out) throws IOException {
		if (out != null)
			out.close();
	}

	public static FileStatus[] setFiles(FileSystem hdfs, Path inputPath) throws IOException {
		if (hdfs.isFile(inputPath))
			return hdfs.listStatus(inputPath.getParent());
		else
			return hdfs.listStatus(inputPath);
	}

}
