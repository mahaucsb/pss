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
 * @Since Aug 13, 2012
 */

package edu.ucsb.cs.partitioning.statistics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

import edu.ucsb.cs.partitioning.PartDriver;
import edu.ucsb.cs.partitioning.cosine.Organizer;
import edu.ucsb.cs.partitioning.jaccard.JaccardCoarsePartitionMain;
import edu.ucsb.cs.types.FeatureWeight;
import edu.ucsb.cs.types.FeatureWeightArrayWritable;
import edu.ucsb.cs.utilities.JobSubmitter;

/**
 * Produces statistics about the data and partitions including maximum, minimum
 * and average vector length and Baraglia dummy vectors for each partition
 * stored in "bragliaVectors" and the whole data dummy vector too.
 * BaragliaPartition vectors are sorted alphabetically, while whole data vector
 * is sorted decreasingly by popularity. It also prints number of skipped edges
 * between partitions and the document pairs comparison omitted.
 * 
 * @author Maha
 */
public class CollectorBaraglia {

	private static TreeMap<Long, Integer> globalFeaturesCount = new TreeMap<Long, Integer>();
	private static FeatureWeightArrayWritable maxWeightVector = new FeatureWeightArrayWritable(); // Baraglia
	public static Path baragliaPath = new Path("bragliaVectors");
	private static MapFile.Writer writer;

	/**
	 * @param input : input path of cosine partitions (Gij ..).
	 */
	public static void printCosineStatistics(JobConf job, String input)
			throws NumberFormatException, IOException {
		FileSystem fs = (new Path(input)).getFileSystem(job);
		printCommonStatistics(fs, input, job);
		String NumSkipPairsEdges = getNSkipCosineVecPairs(fs, new Path(input), job);
		printSkipInfo(new StringTokenizer(NumSkipPairsEdges, " ,"));
	}

	public static void printJaccardStatistics(JobConf job, String input)
			throws NumberFormatException, IOException {
		FileSystem fs = (new Path(input)).getFileSystem(job);
		printCommonStatistics(fs, input, job);
		String NumSkipPairsEdges = getNSkipJaccardDocPairs(fs, new Path(input), job);
		printSkipInfo(new StringTokenizer(NumSkipPairsEdges, " ,"));
	}

	public static void printCommonStatistics(FileSystem fs, String input, JobConf job)
			throws IOException {
		openBaragliaFile();
		String NumMinMaxAvg = getNumMinMaxAvgVecLengthAvgPosting(fs, new Path(input), job);
		StringTokenizer tkz = new StringTokenizer(NumMinMaxAvg, " ,");
		System.out.println(" [Statistics] ");
		System.out.println("  Number of partitions: " + getNumFiles(new Path(input), fs)
				+ "\n  Number of vectors: " + tkz.nextToken() + "\n  Min vector length: "
				+ tkz.nextToken() + "\n  Max vector length: " + tkz.nextToken()
				+ "\n  Avg vector length: " + tkz.nextToken() + "\n  Number of features: "
				+ tkz.nextToken() + "\n  Number of words: " + tkz.nextToken()
				+ "\n  Avg. posting length: " + tkz.nextToken());
	}

	public static void printSkipInfo(StringTokenizer tkz) throws IOException {
		System.out.println("  Number of unique skipped vector pairs: " + tkz.nextToken()
				+ " out of " + tkz.nextToken()
				+ "\n  Number of distinct partitions edges to skip: " + tkz.nextToken()
				+ " out of " + tkz.nextToken() + "\n" + JobSubmitter.stars());
	}

	/**
	 * Prepares a file to contain Baraglia's dummy vector and a dummy vector for
	 * each produced static partition.
	 */
	public static void openBaragliaFile() throws IOException {
		JobConf job = new JobConf();
		FileSystem fs = baragliaPath.getFileSystem(job);
		if (fs.exists(baragliaPath))
			fs.delete(baragliaPath);
		writer = new MapFile.Writer(job, fs, baragliaPath.getName(), Text.class,
				FeatureWeightArrayWritable.class);
	}

	public static FileStatus[] getFiles(Path inputPath, FileSystem fs) throws IOException {

		FileStatus[] files = null;
		if (fs.exists(inputPath)) {
			if (fs.isFile(inputPath)) {
				files = new FileStatus[1];
				files[0] = new FileStatus(0, false, 1, 1, 1, inputPath);
			} else
				files = fs.listStatus(inputPath);
		}
		return files;
	}

	public static int getNumFiles(Path inputPath, FileSystem fs) throws IOException {
		return getFiles(inputPath, fs).length;
	}

	/**
	 * @param inputPath: path of all the input files.
	 * @param fs: file system.
	 * @return file paths sorted by file name.
	 */
	public static Iterator<Path> getSortedFiles(Path inputPath, FileSystem fs) throws IOException {
		TreeSet<Path> paths = new TreeSet<Path>();
		FileStatus[] files = getFiles(inputPath, fs);
		for (int i = 0; i < files.length; i++)
			if (!fs.isDirectory(files[i].getPath()))
				paths.add(files[i].getPath());

		return paths.iterator();
	}

	public static String getNumMinMaxAvgVecLengthAvgPosting(FileSystem fs, Path inputPath,
			JobConf job) throws IOException {

		LongWritable key = new LongWritable();
		FeatureWeightArrayWritable value = new FeatureWeightArrayWritable();
		long numDocuments = 0, minDocLength = Long.MAX_VALUE, maxDocLength = 0, numWords = 0;
		double avgDocLength = 0;

		HashMap<Long, Float> partitionfeaturesWeight = new HashMap<Long, Float>();

		Iterator<Path> pathItr = getSortedFiles(inputPath, fs);
		if (!pathItr.hasNext())
			return "0,0,0,0";

		while (pathItr.hasNext()) {
			inputPath = pathItr.next();
			SequenceFile.Reader in = new SequenceFile.Reader(fs, inputPath, job);
			while (in.next(key, value)) {
				numDocuments++;
				avgDocLength += value.vectorSize;
				if (minDocLength > value.vectorSize)
					minDocLength = value.vectorSize;
				if (maxDocLength < value.vectorSize)
					maxDocLength = value.vectorSize;
				for (int j = 0; j < value.vectorSize; j++) {
					FeatureWeight current = value.vector[j];
					updatePartitionBaraglia(partitionfeaturesWeight, current);
				}
			}
			in.close();
			writePartitionBaraglia(inputPath.getName(), partitionfeaturesWeight);
		}
		long nFeatures = globalFeaturesCount.keySet().size();
		long sumPostings = getSumPostingLength(globalFeaturesCount);
		avgDocLength = avgDocLength / numDocuments;
		return numDocuments + " , " + minDocLength + " , " + maxDocLength + " ," + avgDocLength
				+ " ," + nFeatures + " ," + (float) sumPostings / nFeatures;
	}

	/**
	 * @param featuresCount : vector with all features and the size of their
	 *        postings.
	 * @return sum of of postings lengths of the features.
	 */
	public static long getSumPostingLength(TreeMap<Long, Integer> featuresCount) {
		Iterator<Long> features = featuresCount.keySet().iterator();
		long sumPostings = 0;
		while (features.hasNext())
			sumPostings += featuresCount.get(features.next());
		return sumPostings;
	}

	/**
	 * Writes the passed vector into Baraglia file.
	 * @param featuresWeight : partition features and their maximum weight to be
	 *        written to HDFS and added to Baraglia global vector.
	 * @param partition: name of the partition passed.
	 */
	public static void writePartitionBaraglia(String partition, HashMap<Long, Float> featuresWeight)
			throws IOException {
		maxWeightVector.set(featuresWeight.size());
		Iterator<Long> features = featuresWeight.keySet().iterator();
		int i = -1;
		while (features.hasNext()) {
			long f = features.next();
			maxWeightVector.setElement(++i, f, featuresWeight.get(f));
		}
		writer.append(new Text(partition), maxWeightVector);
		// updateGlobalBaraglia(globalFeaturesCount, featuresWeight);
		featuresWeight.clear();
	}

	/**
	 * @param featuresCount : global vector of features and count to be updated.
	 * @param partitionfeaturesWeight : features and their maximum weight from
	 *        the current partition to be added to the global vector.
	 */
	public static void updateGlobalBaraglia(TreeMap<Long, Integer> globalFeaturesCount,
			HashMap<Long, Float> partitionfeaturesWeight) {

		Iterator<Long> partfeatures = partitionfeaturesWeight.keySet().iterator();
		while (partfeatures.hasNext()) {
			long feature = partfeatures.next();
			if (globalFeaturesCount.containsKey(feature))
				globalFeaturesCount.put(feature, globalFeaturesCount.get(feature) + 1);

			else
				globalFeaturesCount.put(feature, 1);
		}
	}

	/**
	 * @param map : Unsorted map of features to their count and maximum weight.
	 * @return sorted map based on the features count decreasingly by
	 *         popularity.
	 */
	public static SortedSet<Map.Entry<Long, CountWeight>> entriesSortedByPopularity(
			Map<Long, CountWeight> map) {
		SortedSet<Map.Entry<Long, CountWeight>> sortedEntries = new TreeSet<Map.Entry<Long, CountWeight>>(
				new Comparator<Map.Entry<Long, CountWeight>>() {
					public int compare(Map.Entry<Long, CountWeight> e1,
							Map.Entry<Long, CountWeight> e2) {
						return e1.getValue().compareTo(e2.getValue());
					}
				});
		sortedEntries.addAll(map.entrySet());
		return sortedEntries;
	}

	/**
	 * Adds the feature and weight of the current vector to Baraglia vector of
	 * the current partition.
	 * @param featuresWeight:Baraglia vector for the current processed
	 *        partition.
	 * @param current: current vector being read from the partition.
	 */
	public static void updatePartitionBaraglia(HashMap<Long, Float> featuresWeight,
			FeatureWeight current) {
		if (featuresWeight.containsKey(current.feature)) {
			float existing = featuresWeight.get(current.feature);
			if (existing < current.weight) {
				existing = current.weight;
				featuresWeight.put(current.feature, existing);
			}
		} else
			featuresWeight.put(current.feature, current.weight);
	}

	public static boolean skip1dCoarseJaccardPartitions(String cfile, String ofile) {
		Iterator<Integer> itr = JaccardCoarsePartitionMain.skipList.keySet().iterator();
		while (itr.hasNext()) {
			int part1 = itr.next();
			if (Integer.parseInt(cfile) == part1) {
				ArrayList<Integer> iList = JaccardCoarsePartitionMain.skipList.get(part1);
				for (int k = 0; k < iList.size(); k++)
					if (iList.get(k) == Integer.parseInt(ofile))
						return true;
				break;
			}
		}
		return false;
	}

	public static String getNSkipCosineVecPairs(FileSystem fs, Path inputPath, JobConf job)
			throws IOException {

		long nSkipVecPair = 0, nVectors = 0, nSkipPartEdges = 0, nPartitions = 0;
		FileStatus[] files = getFiles(inputPath, fs);
		if (files == null)
			return null;

		for (int i = 0; i < files.length; i++) {
			inputPath = files[i].getPath();
			if (fs.isDirectory(inputPath))
				continue;
			nPartitions++;
			long n = countFileVectors(fs, files[i].getPath(), job);
			nVectors += n;
			for (int j = i; j < files.length; j++) {
				inputPath = files[j].getPath();
				if (fs.isDirectory(inputPath))
					continue;
				long m = countFileVectors(fs, files[j].getPath(), job);
				if (skipCosinePartitions(files[i].getPath().getName(), files[j].getPath().getName())) {
					nSkipVecPair += (n * m);
					nSkipPartEdges++;
				}
			}
		}
		return (nSkipVecPair + ",(" + nVectors + "C2)," + nSkipPartEdges + "," + ",(" + nPartitions + "C2),");
	}

	public static boolean skipCosinePartitions(String cfile, String ofile) {
		int cr = Organizer.getRow(cfile);
		int cc = Organizer.getCol(cfile);
		int or = Organizer.getRow(ofile);
		int oc = Organizer.getCol(ofile);
		if (((cr != cc) && (cc >= or)) || ((or != oc) && (oc >= cr)))
			return true;
		else
			return false;
	}

	/**
	 * Not sure about calculations here anymore ..
	 * @param fs
	 * @param inputPath
	 * @param job
	 * @return
	 * @throws IOException
	 */
	public static String getNSkipJaccardDocPairs(FileSystem fs, Path inputPath, JobConf job)
			throws IOException {

		long nSkipVecPair = 0, nVecPairs = 0, nSkipPartEdges = 0, nPartitions = 0;
		FileStatus[] files = getFiles(inputPath, fs);
		if (files == null)
			return null;

		for (int i = 0; i < files.length; i++) {
			inputPath = files[i].getPath();
			if (fs.isDirectory(inputPath))
				continue;
			nPartitions++;
			for (int j = 0; j < files.length; j++) {
				inputPath = files[j].getPath();
				if (fs.isDirectory(inputPath))
					continue;
				long n = countFileVectors(fs, files[i].getPath(), job);
				long m = countFileVectors(fs, files[j].getPath(), job);

				if (skip1dCoarseJaccardPartitions(files[i].getPath().getName(), files[j].getPath()
						.getName())) {
					nSkipVecPair += (n * m);
					nSkipPartEdges++;
				}
				nVecPairs += (n * m);
			}
		}
		return (nSkipVecPair / 2 + "," + nVecPairs / 2 + "," + nSkipPartEdges / 2 + "," + ",("
				+ nPartitions + "C2),");
	}

	public static long countFileVectors(FileSystem fs, Path inputFile, JobConf job)
			throws IOException {
		long nDocuments = 0;
		LongWritable key = new LongWritable();
		FeatureWeightArrayWritable value = new FeatureWeightArrayWritable();

		if (fs.isDirectory(inputFile))
			return 0;
		SequenceFile.Reader in = new SequenceFile.Reader(fs, inputFile, job);
		while (in.next(key, value))
			nDocuments++;
		in.close();
		return nDocuments;
	}

	public static long countDirVectors(FileSystem fs, Path inputDir, JobConf job)
			throws IOException {
		long nDocuments = 0;
		FileStatus[] files = getFiles(inputDir, fs);
		for (int i = 0; i < files.length; i++)
			nDocuments += countFileVectors(fs, files[i].getPath(), job);
		return nDocuments;
	}

	public static int getNumPartPairs(HashMap<Integer, ArrayList<Integer>> list) {
		int numPairs = 0;
		Iterator<Integer> itr = list.keySet().iterator();
		while (itr.hasNext()) {
			ArrayList<Integer> iList = list.get(itr.next());
			numPairs += iList.size();
		}
		return numPairs / 2; // distinct
	}
}