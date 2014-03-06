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

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;

import edu.ucsb.cs.types.DocDocWritable;

/**
 * This class opens the resulted sequence directories of paired values
 * <code> [DocDocWritable, FloatWritable]<\code> and computes:<br>
 * fn:<br>
 * tn:<br>
 * tp:<br>
 * fp:<br>
 * 
 * @author Maha <br>
 * */
public class FourValues {

	private static HashSet<DocDocWritable> ValidationData = new HashSet<DocDocWritable>();
	private static HashSet<DocDocWritable> TestData = new HashSet<DocDocWritable>();
	private static long allDistinctPairs;

	public static void printUsage() {
		System.out.println("Usage:<ValidationDir> <TestDirectory> <numberDocuments not pairs!>");
		System.exit(0);
	}

	public static void main(String[] args) throws IOException {
		if (args.length != 3)
			printUsage();

		Path validPath = new Path(args[0]);
		Path testPath = new Path(args[1]);
		int n = Integer.parseInt(args[2]);

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
		computeValues(n);

	}

	public static void computeValues(int n) {
		long tp = 0, tn = 0, fp = 0, fn = 0;

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
}
