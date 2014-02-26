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
 * @Since Feb 28, 2013
 */

package edu.ucsb.cs.lsh.projection;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import edu.ucsb.cs.lsh.types.BitSignature;

/**
 * @author Maha. Copied from Ferhan Ture. UMD ivory package.
 */
public class PairOfIntSignature implements WritableComparable<PairOfIntSignature> {
	int permNo;
	BitSignature signature = new BitSignature();

	public PairOfIntSignature() {}

	public PairOfIntSignature(int i, BitSignature permutedSign) {
		permNo = i;
		signature = permutedSign;
	}

	public void readFields(DataInput in) {
		try {
			this.permNo = in.readInt();
			this.signature.readFields(in);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(permNo);
		signature.write(out);
	}

	public int compareTo(PairOfIntSignature other) {
		int c = signature.compareTo(other.signature);
		if (c == 0)
			return (permNo < other.permNo) ? -1 : ((permNo > other.permNo) ? 1 : 0);
		else
			return c;
	}

	public boolean equals(PairOfIntSignature other) {
		return (other.getInt() == getInt() && other.getSignature().equals(this.getSignature()));
	}

	public BitSignature getSignature() {
		return signature;
	}

	public int getInt() {
		return permNo;
	}

	public void setInt(int n) {
		permNo = n;
	}

	public void setSignature(BitSignature s) {
		signature = s;
	}

	@Override
	public String toString() {
		return "(" + permNo + "," + signature + ")";
	}
}
