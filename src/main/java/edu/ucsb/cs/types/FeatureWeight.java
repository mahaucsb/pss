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

import edu.ucsb.cs.types.FeatureWeight;

public class FeatureWeight implements WritableComparable<FeatureWeight> {
	public long feature;
	public float weight;
	private boolean featureSort = true;

	public FeatureWeight() {}

	public FeatureWeight(long l1, float f1) {
		this.feature = l1;
		this.weight = f1;
	}

	public long getFeature() {
		return feature;
	}

	public void setWeightSort() {
		featureSort = false;
	}

	public void setFeatureSort() {
		featureSort = true;
	}

	public float getWeigth() {
		return weight;
	}

	public void setFeature(long l1) {
		this.feature = l1;
	}

	public void setWeight(float f1) {
		this.weight = f1;
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(feature);
		out.writeFloat(weight);
		out.writeBoolean(featureSort);
	}

	public void readFields(DataInput in) throws IOException {
		this.feature = in.readLong();
		this.weight = in.readFloat();
		this.featureSort = in.readBoolean();
	}

	@Override
	public String toString() {
		return (feature + " " + weight);
	}

	public int compareTo(FeatureWeight other) {
		if (featureSort) {// sort by popularity
			if (this.feature < other.feature)
				return -1;
			else if (this.feature > other.feature)
				return 1;
			else
				return 0;
		} else {// Sort decreasingly by weight
			if (this.weight < other.weight)
				return 1;
			else if (this.weight > other.weight)
				return -1;
			else
				return 0;
		}
	}
}
