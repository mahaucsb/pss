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

package edu.ucsb.cs.lsh.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import edu.umd.cloud9.io.map.HashMapWritable;

/**
 * @author Maha
 * 
 */
public class CounterWritable implements WritableComparable {

	public long docid;
	public String SHA1 = null;
	public int totalCount; // total number of words
	public HashMapWritable<LongWritable, IntWritable> featuresCount;

	public CounterWritable() {
		this.featuresCount = new HashMapWritable<LongWritable, IntWritable>();
		this.totalCount = 0;
	}

	public long[] keySet() {
		int nKeys = featuresCount.size();
		long[] keys = new long[nKeys];
		Iterator<LongWritable> l = featuresCount.keySet().iterator();
		for (int i = 0; i < nKeys; i++)
			keys[i] = l.next().get();
		return keys;
	}

	public void addFeature(LongWritable feature) {
		if (featuresCount.containsKey(feature))
			featuresCount.put(feature, featuresCount.get(feature));
		else
			featuresCount.put(feature, new IntWritable(1));
	}

	public int size() {
		return featuresCount.size();
	}

	public boolean containsKey(int key) {
		return featuresCount.containsKey(key);
	}

	public int getCount(long key) {
		return featuresCount.get(key).get();
	}

	public void incrementCount(int key) {
		featuresCount.put(new LongWritable(key), new IntWritable(getCount(key) + 1));
		totalCount++;
	}

	// Sort by signature length
	public int compareTo(Object o) {
		if (o instanceof CounterWritable) {
			return Double.compare(((CounterWritable) o).totalCount, this.totalCount);
		}
		return 0;
	}

	@Override
	public String toString() {
		String s = docid + "=[";
		long[] keys = keySet();
		for (int i = 0; i < size(); i++) {
			s += String.valueOf(keys[i]) + ":" + String.valueOf(getCount(keys[i]))
					+ (i < size() - 1 ? ", " : "");
		}
		s += "] @ " + String.valueOf(totalCount);
		return s;
	}

	public void readFields(DataInput in) throws IOException {
		docid = in.readLong();
		SHA1 = in.readUTF();
		totalCount = in.readInt();
		featuresCount.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(docid);
		out.writeUTF(SHA1);
		out.writeInt(totalCount);
		featuresCount.write(out);
	}
}
