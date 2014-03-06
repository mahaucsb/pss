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

import edu.ucsb.cs.types.FeatureWeight;
import edu.ucsb.cs.types.IndexFeatureWeight;

/**
 * @author Maha
 * 
 */
public class IndexFeatureWeight extends FeatureWeight {
	public int index;

	public IndexFeatureWeight() {
		super();
	}

	public IndexFeatureWeight(int i1, long l1, float f1) {
		super(l1, f1);
		this.index = i1;
	}

	public void set(int i1, long f1, float w1) {
		this.index = i1;
		this.feature = f1;
		this.weight = w1;
	}

	public void setFeatureWeight(long f1, float w1) {
		this.feature = f1;
		this.weight = w1;
	}

	public void setIndex(int i1) {
		this.index = i1;
	}

	@Override
	public String toString() {
		return index + " " + super.toString();
	}

	public int compareTo(IndexFeatureWeight other) {
		if (this.feature < other.feature)
			return -1;
		else if (this.feature > other.feature)
			return 1;
		else
			return 0;
	}
}
