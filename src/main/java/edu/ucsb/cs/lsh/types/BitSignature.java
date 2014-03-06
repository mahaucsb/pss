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
 * @Since Jan 2, 2013
 */

package edu.ucsb.cs.lsh.types;

import ivory.lsh.data.Bits;
import ivory.lsh.data.NBitSignature;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import edu.ucsb.cs.types.FeatureWeightArrayWritable;

/**
 * @author Maha
 * 
 */
public class BitSignature extends NBitSignature implements WritableComparable { // NBitSignature
	public long docno;
	public FeatureWeightArrayWritable vector = new FeatureWeightArrayWritable();

	public BitSignature() {}

	public BitSignature(int size) {
		super(size);
	}

	public BitSignature(Bits b) {
		super(b);
	}

	public BitSignature(BitSignature other) {
		super(other);
	}

	public BitSignature(byte[] bits, int len) {
		super(bits, len);
	}

	public void setId(long docno) {
		this.docno = docno;
	}

	public void setVector(FeatureWeightArrayWritable v) {
		this.vector = v;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		this.docno = in.readLong();
		vector.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeLong(docno);
		vector.write(out);
	}

	public int compareTo(BitSignature other) {
		boolean myBit, otherBit;
		for (int i = 0; i < size(); i++) {
			myBit = get(i);
			otherBit = other.get(i);
			if (!myBit && otherBit) {
				return -1;
			} else if (myBit && !otherBit) {
				return 1;
			}
		}
		return 0;
	}

	public int convertToInt() {
		String s = new StringBuffer(toString()).reverse().toString();
		int num = 0, mul = 1;
		for (int i = 0; i < s.length(); i++) {
			int c = Integer.parseInt(s.charAt(i) + "");
			num += mul * c;
			mul *= 2;
		}
		return num;
	}

	@Override
	public String toString() {
		StringBuilder bdr = new StringBuilder(super.toString());
		bdr.append(" docno:" + docno + ", vector:" + vector.toString() + " ");
		return bdr.toString(); // remove comment
	}
}
