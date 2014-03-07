package edu.ucsb.cs.preprocessing.sequence;

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
 * @Since Aug 4, 2012
 */

/**
 * @author Maha
 * This class prepares input into DBLP format
 */
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;

import edu.ucsb.cs.types.FeatureWeightArrayWritable;

/*
 * To combine sequence files and produce one file of the size specified residing in HDFS. 
 */
public class SeqFilesCombiner {
	public static void main(String args[]) throws IOException {
		if (args.length != 3) {
			System.out.println("Usage:preprocessing.jar seq combine <inputDir> <outputFile> <nLines> to combine sequence files"
					+ " \n| <inputDir> to calcualte average vector size");
		} else 
			readDirWriteFile(args[0], args[1], Integer.parseInt(args[2]));
	}

	public static void readDirWriteFile(String inputDir, String outputFile, int nLines)
			throws IOException {
		Configuration conf = new Configuration();
		Path inputPath = new Path(inputDir);
		FileSystem fs = inputPath.getFileSystem(conf);
		int lineCount = 0;
		FileStatus[] files = fs.listStatus(inputPath);
		if (fs.exists(new Path(outputFile)))
			fs.delete(new Path(outputFile));

		SequenceFile.Writer outFile = SequenceFile.createWriter(fs, conf, new Path(outputFile),
				LongWritable.class, FeatureWeightArrayWritable.class,
				SequenceFile.CompressionType.NONE);

		for (int i = 0; i < files.length; i++) {
			if (files[i].isDir() || files[i].getPath().getName().startsWith("_"))
				continue;
			Reader reader = new SequenceFile.Reader(fs, files[i].getPath(), conf);
			LongWritable key = new LongWritable();
			FeatureWeightArrayWritable value = new FeatureWeightArrayWritable();

			while (reader.next(key, value) && lineCount < nLines) {
				lineCount++;
				outFile.append(key, value);
			}
			reader.close();
			if (lineCount >= nLines)
				break;
		}
		outFile.close();
		System.out.println("Total lines copied: " + lineCount);
	}

	///This should be moved to read
	public static void getAvgVectorSize(String inputDir) throws IOException {
		long sumFeatures = 0, nLines = 0;
		Configuration conf = new Configuration();
		Path inputPath = new Path(inputDir);
		FileSystem fs = inputPath.getFileSystem(conf);
		int lineCount = 0;
		if (!fs.exists(new Path(inputDir)))
			System.out.println("InputDir DNE!");

		FileStatus[] files = fs.listStatus(inputPath);

		for (int i = 0; i < files.length; i++) {
			if (files[i].isDir() || files[i].getPath().getName().startsWith("_"))
				continue;
			Reader reader = new SequenceFile.Reader(fs, files[i].getPath(), conf);
			LongWritable key = new LongWritable();
			FeatureWeightArrayWritable value = new FeatureWeightArrayWritable();

			while (reader.next(key, value)) {
				sumFeatures += value.getSize();
				lineCount++;
			}
			reader.close();
			if (lineCount >= nLines)
				break;
		}
		System.out.println("Avg. vector length: " + (float) sumFeatures / (float) lineCount);
	}

}