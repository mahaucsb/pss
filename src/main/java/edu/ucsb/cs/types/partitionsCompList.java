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
 * @Since Sep 12, 2013
 */

package edu.ucsb.cs.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * @author Maha
 * 
 */
public class partitionsCompList implements Writable {

	Text name;
	IntWritable numComparisons;
	ArrayList<Text> partitionsList;

	public partitionsCompList(String partition) {
		name = new Text(partition);
		partitionsList = new ArrayList<Text>();
		numComparisons = new IntWritable(0);
	}

	public void addPartition(String partition) {
		partitionsList.add(new Text(partition));
		numComparisons.set(partitionsList.size());
	}

	public boolean removePartition(String partition) {
		for (int i = 0; i < numComparisons.get(); i++)
			if (partitionsList.get(i).toString().equalsIgnoreCase(partition.toLowerCase())) {
				partitionsList.remove(i);
				numComparisons.set(partitionsList.size());
				return true;
			}
		return false;
	}

	public void readFields(DataInput in) throws IOException {
		this.name.readFields(in);
		this.numComparisons.readFields(in);
		for (int i = 0; i < this.numComparisons.get(); i++)
			this.partitionsList.get(i).readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		this.name.write(out);
		this.numComparisons.write(out);
		for (int i = 0; i < this.numComparisons.get(); i++)
			this.partitionsList.get(i).write(out);
	}
}
