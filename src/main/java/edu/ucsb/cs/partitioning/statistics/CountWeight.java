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
 * @Since Aug 23, 2012
 */

package edu.ucsb.cs.partitioning.statistics;

/**
 * @author Maha
 * 
 */
public class CountWeight implements Comparable<CountWeight> {

	public int count;
	public float weight;

	CountWeight(int i, float f) {
		count = i;
		weight = f;
	}

	public void incCount() {
		count++;
	}

	public void setWeight(float f) {
		weight = f;
	}

	/**
	 * Sorting by popularity to be used for finding Baraglia vector.
	 */
	public int compareTo(CountWeight o) {
		if (this.count > o.count)
			return -1;
		else if (this.count < o.count)
			return 1;
		else
			return -1;
	}
}
