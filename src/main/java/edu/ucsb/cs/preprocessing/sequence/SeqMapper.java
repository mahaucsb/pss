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
 * @Since Jul 27, 2012
 */

package edu.ucsb.cs.preprocessing.sequence;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.ucsb.cs.hybrid.types.FeatureWeight;
import edu.ucsb.cs.hybrid.types.FeatureWeightArrayWritable;

/**
 * Change: it here to save mapping from original id to md5 hash! <br>
 * <code>
 * input: 001 1 0.1 2 0.3 ... <br> 
 * output: MD5(001) 1 0.1 2 0.3 ...
 * </code>
 */
public class SeqMapper extends MapReduceBase implements
		Mapper<Object, Text, LongWritable, FeatureWeightArrayWritable> {

	private LongWritable id = new LongWritable();
	private FeatureWeightArrayWritable vector = new FeatureWeightArrayWritable();

	public void map(Object key, Text value,
			OutputCollector<LongWritable, FeatureWeightArrayWritable> output, Reporter reporter)
			throws IOException {
		setKey(value.toString());
		setValue(value.toString());
		output.collect(id, vector);
	}

	private void setKey(String record) {
		StringTokenizer tkz = new StringTokenizer(record);
		id.set(getMD5(tkz.nextToken(), 8));
	}

	private void setValue(String record) {
		StringTokenizer tkz = new StringTokenizer(record);
		tkz.nextToken();// id
		ArrayList<FeatureWeight> array = new ArrayList<FeatureWeight>();

		while (tkz.hasMoreTokens())
			array.add(new FeatureWeight(Long.parseLong(tkz.nextToken()), Float.parseFloat(tkz
					.nextToken())));
		vector.vectorSize = array.size();
		FeatureWeight[] toArray = new FeatureWeight[array.size()];
		array.toArray(toArray);
		vector.vector = toArray;
	}

	private static long getMD5(String s, int n) {
		byte[] result = new byte[n];
		try {
			byte[] bytesOfMessage = s.getBytes("UTF-8");
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.reset();
			md.update(bytesOfMessage);
			byte[] thedigest = md.digest(bytesOfMessage);
			for (int i = 0; i < n; i++)
				result[i] = thedigest[i];

			ByteArrayInputStream bos = new ByteArrayInputStream(result);
			DataInputStream dos = new DataInputStream(bos);
			return (dos.readLong());
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

}
