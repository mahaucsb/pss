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

import org.apache.hadoop.io.Writable;

public class DocWeight implements Writable {
	public long docId;
	public float weight;

	public DocWeight() {}

	public DocWeight(long l1, float f1) {
		this.docId = l1;
		this.weight = f1;
	}

	public long getdocId() {
		return docId;
	}

	public float getWeigth() {
		return weight;
	}

	public void setLong(long l1) {
		this.docId = l1;
	}

	public void setFloat(float f1) {
		this.weight = f1;
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(docId);
		out.writeFloat(weight);
	}

	public void readFields(DataInput in) throws IOException {
		this.docId = in.readLong();
		this.weight = in.readFloat();
	}
}
