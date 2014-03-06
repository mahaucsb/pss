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
 * @Since Aug 1, 2012
 */

package edu.ucsb.cs.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import edu.ucsb.cs.types.IntIntWritable;

public class IntIntWritable implements WritableComparable<IntIntWritable> {
	public int x;
	public int y;

	public IntIntWritable() {}

	public IntIntWritable(int x1, int y1) {
		x = x1;
		y = y1;
	}

	public IntIntWritable(int x1) {
		x = x1;
	}

	public void set(int x1, int y1) {
		x = x1;
		y = y1;
	}

	@Override
	public String toString() {
		return x + " " + y;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(x);
		out.writeInt(y);
	}

	public void readFields(DataInput in) throws IOException {
		x = in.readInt();
		y = in.readInt();
	}

	public int compareTo(IntIntWritable other) {
		if (x < other.x)
			return -1;
		else if (x > other.x)
			return 1;
		else if (y < other.y)
			return -1;
		else if (y > other.y)
			return 1;
		else
			return 0;
	}
}
