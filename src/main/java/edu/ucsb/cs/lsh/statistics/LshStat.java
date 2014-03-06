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
 * @Since Sep 3, 2012
 */

package edu.ucsb.cs.lsh.statistics;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;

import edu.ucsb.cs.types.DocDocWritable;
import edu.ucsb.cs.types.FeatureWeightArrayWritable;

/**
 * This class offers the following options:<br>
 * 1) Computes LSH four values. It takes a valid directory with paired documents
 * exceeding threshold. Another test directory to compute its errors from the
 * valid directory. Input is of the format:
 * <code> [DocDocWritable, FloatWritable]<\code> and computes:<br>
 * Ouptut are the following values:
 * fn: wrong pairs.<br>
 * tn: missing pairs.<br>
 * tp: correct present pairs.<br>
 * fp: wrong present pairs.<br>
 * 
 * 2) Convert text input to binary DocDocWritable,FloatWritable.
 * 
 * 
 * @author Maha <br>
 * */
public class LshStat {

	public static void printUsage(int choice) {
		switch (choice) {
			case 1:
				System.out.println("Usage: 1 <ValidDir> <TestDir> <numberDocuments not pairs>");
				break;
			case 2:
				System.out.println("Usage: 2 <Local text inputDir> <outputDir>");
				break;
			case 3:
				System.out.println("Usage: 3 <hdfs lsh directory> [yes for maxBucket]");
				break;
			case 4:
				System.out.println("Usage: 4 <hdfs lsh directory> [<bucketNo>]");
				break;
			default:
		}
		System.exit(0);
	}

	private static long maxBucketID = 0;

	public static void main(String[] args) throws IOException {
		if ((args.length < 1) || !args[0].matches("[\\d.]+")) {
			System.out
					.println("Enter number:\n"
							+ "(1) - compute LSH four values from binary input <DocDoc,Float>.\n"
							+ "(2) - convert text input to <DocDoc,FloatWritable>.\n"
							+ "(3) - lsh statistics Max/Min/Avg/buckets/repetition from binary input from binary input <DocDoc,Float>.\n"
							+ "(4) - produce maximum bucket.\n");

			System.exit(0);
		}
		switch (Integer.parseInt(args[0])) {
			case 1:
				computeValues(args);
				break;
			case 2:
				convertInput(args);
				break;
			case 3:
				lshProjectionStat(args);
				break;
			case 4:
				produceMaxBucket(args);
				break;
		}

	}

	private static HashSet<DocDocWritable> ValidationData = new HashSet<DocDocWritable>();
	private static HashSet<DocDocWritable> TestData = new HashSet<DocDocWritable>();
	private static long allDistinctPairs;

	public static void computeValues(String[] args) throws IOException {
		if (args.length != 4)
			printUsage(1);

		long tp = 0, tn = 0, fp = 0, fn = 0;

		Path validPath = new Path(args[1]);
		Path testPath = new Path(args[2]);
		int n = Integer.parseInt(args[3]);

		Configuration conf = new Configuration();
		FileSystem fs = testPath.getFileSystem(conf);
		long validPairCount = 0, testPairCount = 0;
		allDistinctPairs = (n * (n - 1)) / 2;

		Reader validReader = new SequenceFile.Reader(fs, validPath, conf);
		DocDocWritable key = new DocDocWritable();
		FloatWritable value = new FloatWritable();

		// Read data into sets
		while (validReader.next(key, value)) {
			validPairCount++;
			assert (key.doc1 < key.doc2);
			ValidationData.add(key);
		}

		Reader testReader = new SequenceFile.Reader(fs, testPath, conf);
		while (testReader.next(key, value)) {
			testPairCount++;
			assert (key.doc1 < key.doc2);
			TestData.add(key);
		}

		// Compute tp , fp
		Iterator<DocDocWritable> testItr = TestData.iterator();
		while (testItr.hasNext()) {
			DocDocWritable next = testItr.next();
			if (ValidationData.contains(next)) {
				tp++;
				ValidationData.remove(next);
			} else
				fp++;
		}

		// Compute fn
		Iterator<DocDocWritable> validItr = ValidationData.iterator();
		while (validItr.hasNext()) {
			testItr.next();
			fn++;
		}

		// Compute tn
		tn = allDistinctPairs - (tp + fn + fp);
		System.out.println("Number of documents: " + n + "# tp: " + tp + " (" + tp
				/ allDistinctPairs + " %)" + "# fp: " + fp + " (" + fp / allDistinctPairs + " %)"
				+ "# tn: " + tn + " (" + tn / allDistinctPairs + " %)" + "# fn: " + fn + " (" + fn
				/ allDistinctPairs + " %)");
	}

	public static void convertInput(String[] args) throws IOException {

		if (args.length != 3)
			printUsage(2);

		String strLine, input = args[1], output_file = args[2];
		Path outPath = new Path(output_file);
		Configuration conf = new Configuration();
		FileSystem fs = outPath.getFileSystem(conf);
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, outPath,
				DocDocWritable.class, FloatWritable.class, SequenceFile.CompressionType.NONE);

		if ((new File(input)).isDirectory()) {
			for (File inputFile : (new File(input)).listFiles()) {
				BufferedReader br = new BufferedReader(new InputStreamReader(new DataInputStream(
						new FileInputStream(inputFile))));

				while ((strLine = br.readLine()) != null) {
					writer.append(new DocDocWritable(0, 3), new FloatWritable(1));
				}
			}
		} else {}
		writer.close();
	}

	public static void lshProjectionStat(String[] args) throws IOException {
		boolean produceMax = false;
		if (args.length == 3)
			produceMax = true;
		else if (args.length != 2)
			printUsage(3);

		Path inputPath = new Path(args[1]);
		Configuration conf = new Configuration();
		FileSystem fs = inputPath.getFileSystem(conf);
		FileStatus[] files = fs.listStatus(inputPath);
		long i = 0, bucketCount = 0, avgBucketSize = 0, maxBucket = 0, minBucket = Long.MAX_VALUE;
		ArrayList<Integer> bucketSizes = new ArrayList<Integer>();

		for (FileStatus file : files) {
			if ((fs.isDirectory(file.getPath())) || file.getPath().getName().startsWith("_"))
				continue;

			Reader reader = new SequenceFile.Reader(fs, file.getPath(), conf);
			LongWritable key = new LongWritable();
			FeatureWeightArrayWritable value = new FeatureWeightArrayWritable();

			while (reader.next(key, value)) {
				if (key.get() == 0) {
					bucketCount++;
					avgBucketSize += i;
					if (maxBucket < i) {
						maxBucket = i;
						maxBucketID = (bucketCount - 1);
					}
					if (i != 0 && minBucket > i)
						minBucket = i;
					i = 0;
				} else {
					i++;
				}
			}
			avgBucketSize += i;
			bucketSizes.add((int) i);
		}
		System.out.println("Number of buckets:" + bucketCount);
		System.out.println("Max. bucket size:" + maxBucket + " with ID:" + maxBucketID);
		System.out.println("Min. bucket size:" + minBucket);
		System.out.println("Avg. buckets size:" + (avgBucketSize / (float) bucketCount));
		System.out.println("R-std. among bucket sizes:"
				+ getRStd((avgBucketSize / (float) bucketCount), bucketSizes));
		System.out.println("Total comparison done within buckets:" + getSumCombin(bucketSizes));
		if (produceMax)
			produceMaxBucket(args);
		// getRepatedPairs(files, fs, conf);
	}

	public static float getRStd(float avg, ArrayList<Integer> array) {
		double sum = 0;
		for (int i = 0; i < array.size(); i++)
			sum += Math.pow(array.get(i) - avg, 2);
		double sd = Math.sqrt(sum / (array.size() - 1));
		System.out.println("Standard deviation across buckets: " + sd);
		return (float) (sd / avg);
	}

	public static long getSumCombin(ArrayList<Integer> array) {
		long sum = 0;
		for (int i = 0; i < array.size(); i++)
			sum += choose(array.get(i), 2);
		return sum;
	}

	// java heap space
	public static void getRepatedPairs(FileStatus[] files, FileSystem fs, Configuration conf)
			throws IOException {
		NumByteList bucket = null;
		long i = 0, bucketCount = 0;
		ArrayList<NumByteList> buckets = new ArrayList<NumByteList>();

		for (FileStatus file : files) {
			if ((fs.isDirectory(file.getPath())) || file.getPath().getName().startsWith("_"))
				continue;

			Reader reader = new SequenceFile.Reader(fs, file.getPath(), conf);
			LongWritable key = new LongWritable();
			FeatureWeightArrayWritable value = new FeatureWeightArrayWritable();

			while (reader.next(key, value)) {
				if (key.get() == 0) {
					if (bucketCount != 0)
						buckets.add(bucket);
					bucketCount++;
					bucket = new NumByteList(bucketCount);
					i = 0;
				} else {
					i++;
					bucket.addDoc(key.get());
				}
			}
		}
		System.out.println("Number of repeated docs across buckets: "
				+ getRepetedPairsCount(buckets));
	}

	public static int getBitId(Long doc) {
		return 0;
	}

	public static void produceMaxBucket(String args[]) throws IOException {
		if (args.length == 3)
			maxBucketID = Integer.parseInt(args[2]);
		else if (args.length != 2)
			printUsage(4);

		Path inputPath = new Path(args[1]);
		Path outPath = new Path("maxBucket");
		Configuration conf = new Configuration();
		FileSystem fs = inputPath.getFileSystem(conf);
		if (fs.exists(outPath))
			fs.delete(outPath);
		FileStatus[] files = fs.listStatus(inputPath);
		SequenceFile.Writer writer = null;
		int bucketCount = 0;

		for (FileStatus file : files) {
			if ((fs.isDirectory(file.getPath())) || file.getPath().getName().startsWith("_"))
				continue;

			Reader reader = new SequenceFile.Reader(fs, file.getPath(), conf);
			LongWritable key = new LongWritable();
			FeatureWeightArrayWritable value = new FeatureWeightArrayWritable();

			while (reader.next(key, value))
				if (key.get() == 0) {
					bucketCount++;
					if (bucketCount == maxBucketID) {
						writer = SequenceFile
								.createWriter(fs, conf, outPath, LongWritable.class,
										FeatureWeightArrayWritable.class,
										SequenceFile.CompressionType.NONE);
						while (reader.next(key, value) && (key.get() != 0))
							writer.append(key, value);
						writer.close();
						return;
					}
				}
		}
	}

	public static Long getRepetedPairsCount(ArrayList<NumByteList> buckets) {
		long pairCount = 0;
		for (int i = 0; i < buckets.size() - 1; i++)
			for (int j = i + 1; j < buckets.size(); j++) {
				NumByteList bucketi = buckets.get(i);
				NumByteList bucketj = buckets.get(j);
				for (int k = 0; k < bucketi.getNumBytes() && k < bucketj.getNumBytes(); k++)
					pairCount += choose(intersectCount(bucketi.getByte(k), bucketj.getByte(k)), 2);
			}
		return pairCount;
	}

	public static int intersectCount(byte a, byte b) {
		int ones, value = (a & b), sum = 0;
		for (int i = 0; i < 8; i++) {
			ones = (value >> i) & 1;
			if (ones > 1)
				sum += ones;
		}
		return sum;
	}

	private static Boolean isBitSet(byte b, int bit) {
		return (b & (1 << bit)) != 0;
	}

	public static double choose(int x, int y) {
		if (y < 0 || y > x)
			return 0;
		if (y > x / 2) {
			y = x - y;
		}

		double denominator = 1.0, numerator = 1.0;
		for (int i = 1; i <= y; i++) {
			denominator *= i;
			numerator *= (x + 1 - i);
		}
		return numerator / denominator;
	}
}

class NumByteList {
	long num;
	ArrayList<Byte> bytes = new ArrayList<Byte>(0);

	public NumByteList(long n) {
		num = n;
	}

	public void addDoc(Long docId) {
		int bytenum = (int) Math.floor(docId / 8);
		byte tochange = 0;
		if (bytes.size() < bytenum) {
			for (int j = bytes.size(); j < bytenum; j++)
				bytes.add((byte) 0);
			tochange = 0;
		} else
			tochange = getByte(bytenum);
		tochange = changeBit(tochange, (int) (docId % 8));
	}

	/**
	 * 
	 * @param tochange
	 * @param i
	 * @return a byte with bit-i set to 1.
	 */
	public byte changeBit(byte tochange, int i) {
		tochange = (byte) (tochange | (1 << i));
		return tochange;
	}

	public int getNumBytes() {
		return bytes.size();
	}

	public byte getByte(int i) {
		return bytes.get(i);
	}
}
