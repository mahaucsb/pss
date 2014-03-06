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
 * @Since Jul 26, 2012
 */

package edu.ucsb.cs.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import edu.ucsb.cs.types.DocDocWritable;

public class DocDocWritable implements WritableComparable<DocDocWritable> {

	public long doc1;
	public long doc2;

	public DocDocWritable() {}

	public DocDocWritable(long d1, long d2) {
		this.doc1 = d1;
		this.doc2 = d2;
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(this.doc1);
		out.writeLong(this.doc2);
	}

	public void readFields(DataInput in) throws IOException {
		this.doc1 = in.readLong();
		this.doc2 = in.readLong();
	}

	@Override
	public String toString() {
		return new String(doc1 + " " + doc2 + " ");
	}

	public long getDoc1() {
		return this.doc1;
	}

	public long getDoc2() {
		return this.doc2;
	}

	public void setDoc1(long doc) {
		this.doc1 = doc;
	}

	public void setDoc2(long doc) {
		this.doc2 = doc;
	}

	public int compareTo(DocDocWritable other) {
		if (this.doc1 < other.doc1)
			return -1;
		else if (this.doc1 > other.doc1)
			return 1;
		else {
			if (this.doc2 < other.doc2)
				return -1;
			else if (this.doc2 > other.doc2)
				return 1;
			else
				return 0;
		}
	}
}
