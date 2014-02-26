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
 * @Since Sep 8, 2012
 */

package edu.ucsb.cs.lsh.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * @author Maha
 */

public class IntArrayWritable implements WritableComparable<IntArrayWritable> {

	private int size;
	private int[] array;

	public IntArrayWritable() {}

	public IntArrayWritable(int[] a) {
		this.size = a.length;
		this.array = a;
	}

	public void setArray(int[] a) {
		this.size = a.length;
		this.array = a;
	}

	public int compareTo(IntArrayWritable other) {
		int smaller = this.size < other.size ? this.size : other.size;
		for (int i = 0; i < smaller; i++)
			if (this.array[i] < other.array[i])
				return -1;
			else if (this.array[i] > other.array[i])
				return 1;
		if (smaller <= this.size)
			return 1;
		else if (smaller <= other.size)
			return -1;
		else
			return 0;
	}

	public void readFields(DataInput in) throws IOException {
		this.size = in.readInt();
		this.array = new int[size];
		for (int i = 0; i < size; i++)
			array[i] = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(size);
		for (int i = 0; i < size; i++)
			out.writeInt(array[i]);
	}
}

// public class IntArrayWritable extends ArrayWritable implements
// Comparable<IntArrayWritable> {
//
// public IntArrayWritable() {
// super(IntWritable.class);
// }
//
// public IntArrayWritable(IntWritable[] array) {
// super(IntWritable.class, array);
// }
//
// public void setArray(int[] array) {
// IntWritable[] a = new IntWritable[array.length];
// for (int i = 0; i < array.length; i++)
// a[i] = new IntWritable(array[i]);
// super.set(a);
// }
//
// public int compareTo(IntArrayWritable other) {
// return (this.compareTo(other));
// }
// }
