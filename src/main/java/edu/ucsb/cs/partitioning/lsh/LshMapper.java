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
 * @Since Sep 5, 2012
 */

package edu.ucsb.cs.partitioning.lsh;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * This class reads in documents of the format [docid word1 word2 ...] and
 * produce an lsh minhash signature to each of them "l" times using a list of
 * minHashTables.
 * 
 * @author Maha
 */
public class LshMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, IntArrayWritable, LongWritable> {

	CounterWritable docCount = new CounterWritable();
	private LongWritable lng = new LongWritable();
	private IntArrayWritable keySig = new IntArrayWritable();
	private int l;
	private LshTable lsh = new LshTable();

	@Override
	public void configure(JobConf job) {
		l = job.getInt(LshPartitionMain.L_PROPERTY, LshPartitionMain.L_VALUE);
		try {
			Path[] localFiles = DistributedCache.getLocalCacheFiles(job);
			// System.out.println("local:" + localFiles[0].getName());
			// FileSystem fs = localFiles[0].getFileSystem(job);
			FileSystem fs = FileSystem.get(job);
			// Reader reader = new SequenceFile.Reader(fs, localFiles[0], job);
			Reader reader = new SequenceFile.Reader(fs, new Path("lshfile"), job);
			reader.next(lsh);
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void map(LongWritable unused, Text idwordsBag,
			OutputCollector<IntArrayWritable, LongWritable> output, Reporter report)
			throws IOException {
		StringTokenizer tkz = new StringTokenizer(idwordsBag.toString(), " ");
		docCount.docid = Long.parseLong(tkz.nextToken()); // document id
		while (tkz.hasMoreTokens()) {
			String word = tkz.nextToken();
			lng.set(Long.parseLong(word));
			docCount.addFeature(lng);
		}
		ArrayList<int[]> signatures = lsh.getSignatures(docCount);
		lng.set(docCount.docid);
		System.out.println("Num sigs: " + signatures.size());
		for (int i = 0; i < l; i++) {
			keySig.setArray(signatures.get(i));
			output.collect(keySig, lng);
		}
	}
}
