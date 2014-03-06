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
 * @Since Nov 14, 2012
 */

package edu.ucsb.cs.hybrid.io;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;

import edu.ucsb.cs.hybrid.Config;
import edu.ucsb.cs.partitioning.cosine.Partitioner;
import edu.ucsb.cs.partitioning.statistics.Collector;
import edu.ucsb.cs.types.FeatureWeightArrayWritable;

/**
 * @author Maha
 */
public class Splitter {

	public static final String OTHERS_INPUT = "staticpartitions-others";

	private static LongWritable key = new LongWritable();
	private static FeatureWeightArrayWritable value = new FeatureWeightArrayWritable();
	private static FileSystem hdfs = null;

	/*
	 * inputPath is usually "staticpartitions1"
	 */
	public static void configure(JobConf job, Path inputPath) throws IOException {
		hdfs = FileSystem.get(job);
		int initS = job.getInt(Config.MAP_S_PROPERTY, Config.MAP_S_VALUE);
		long nVectors = Collector.countDirVectors(hdfs, inputPath, job);
		if (initS > nVectors)
			try{
			throw new UnsupportedEncodingException(
					"WARNING: Mapper's host partition \"S\" is larger than the total number of input vectors! ");
			}catch(UnsupportedEncodingException e){;}

		if (job.getBoolean(Config.SINGLE_MAP_PROPERTY, Config.SINGLE_MAP_VALUE)){
		    prepareOneMap(job, inputPath, initS); 
		}else {
			long splitSize = getSplitSize(nVectors, initS);
			Path splitsDir = splitAll(job, splitSize, inputPath);
			hdfs.delete(inputPath, true);
			hdfs.rename(splitsDir, inputPath);
		}
	}

	/**
	 * @param nVectors: total number of vectors in input.
	 * @param initialS : initial number of vectors per map tasks.
	 * @return modified s such that it is distributed equally among the maps.
	 */
	public static long getSplitSize(long nVectors, long initialS) {
		if (nVectors % initialS == 0)
			return initialS;
		else {
			long nPartitions = ((nVectors / initialS)== 0)? 1:(nVectors / initialS);
			return (initialS + (long) Math.ceil((nVectors % initialS) / nPartitions));
		}
	}

	/**
	 * @param inputPath: path is moved to "others" path and left with one file
	 *        of size "splitSize" in it as input to one map task.
	 * @param splitSize: s vectors per map task.
	 */
	public static void prepareOneMap(JobConf job, Path inputPath, long splitSize)
			throws IOException {
		Path othersPath = new Path(OTHERS_INPUT);
		hdfs.delete(othersPath, true);
		hdfs.rename(inputPath, new Path(OTHERS_INPUT));
		hdfs.delete(inputPath, true);
		hdfs.mkdirs(inputPath);
		createOneMapFile(job, inputPath, othersPath, splitSize);
	}

	/**
	 * Checks input files and picks one with the requested splitSize.
	 * @param job : job configuration.
	 * @param inputPath: path to contain the one map file.
	 * @param othersPath: other path that contains the whole input.
	 * @param splitSize: s vectors put into one map file.
	 */
	public static void createOneMapFile(JobConf job, Path inputPath, Path othersPath, long splitSize)
			throws IOException {
		FileStatus[] files = hdfs.listStatus(othersPath);
		for (int i = 0; i < files.length; i++) {
			if (Collector.countFileVectors(hdfs, files[i].getPath(), job) >= splitSize) {
				SequenceFile.Reader reader = new SequenceFile.Reader(hdfs, files[i].getPath(), job);
				SequenceFile.Writer writer = SequenceFile.createWriter(hdfs, job, new Path(
						inputPath.getName() + "/" + files[i].getPath().getName()),
						LongWritable.class, FeatureWeightArrayWritable.class,
						SequenceFile.CompressionType.NONE);

				long vCount = -1;
				while (reader.next(key, value) && (++vCount) < splitSize)
					writer.append(key, value);
				writer.close();
				return;
			}
		}
		throw new UnsupportedEncodingException("SplitSize requested is larger than each file !");
	}

	/**
	 * splits the files in the passed input directory into at most s vectors
	 * each. It does not combine the vectors from two different partitions.
	 * @param job : configurations.
	 * @param splitSize : split files into at most this size of vectors.
	 * @param inputPath : path of the directory of the input files.
	 * @return path of the splitted files with each at most s vectors.
	 */
	public static Path splitAll(JobConf job, long splitSize, Path inputPath) throws IOException {

		System.out.println("Splitter.splitAll() from " + inputPath.getName());
		LongWritable key = new LongWritable();
		FeatureWeightArrayWritable value = new FeatureWeightArrayWritable();
		SequenceFile.Writer writer = null;

		String tmpDir = "splits-tmp";
		hdfs.delete(new Path(tmpDir), true);
		hdfs.mkdirs(new Path(tmpDir));

		FileStatus[] files = Partitioner.setFiles(hdfs, inputPath);
		for (int i = 0; i < files.length; i++) {
			if ((hdfs.isDirectory(files[i].getPath()) || files[i].getPath().getName()
					.startsWith("_")))
				continue;
			SequenceFile.Reader reader = new SequenceFile.Reader(hdfs, files[i].getPath(), job);
			long subpartition = 0, vecCount = 0;

			while (reader.next(key, value)) {
				vecCount++;
				if (vecCount == 1) {
					if (writer != null)
						writer.close();
					subpartition++;
					writer = SequenceFile.createWriter(hdfs, job, new Path(tmpDir + "/"
							+ files[i].getPath().getName() + "-" + subpartition),
							LongWritable.class, FeatureWeightArrayWritable.class,
							SequenceFile.CompressionType.NONE);

				}
				writer.append(key, value);
				if (vecCount == splitSize)
					vecCount = 0;
			}
		}
		writer.close();
		return new Path(tmpDir);
	}

	public void produceBalagriaVectors() {

	}
}
