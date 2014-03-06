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

import edu.ucsb.cs.types.PostingDocWeight;


public class PostingDocWeight {
	public int doc;
	public float weight;

	PostingDocWeight() {}

	public PostingDocWeight(int d, float w) {
		this.doc = d;
		this.weight = w;
	}

	@Override
	public String toString() {
		return new String(this.doc + " " + this.weight + " ");
	}

	public void addWeight(float w) {
		weight += w;
	}

	public int getDoc() {
		return this.doc;
	}

	public float getWeight() {
		return this.weight;
	}

	public void setDoc(int d) {
		this.doc = d;
	}

	public void setWeight(float f) {
		this.weight = f;
	}

	public int compareTo(PostingDocWeight other) {

		// return Integer.valueOf(doc).compareTo(other.doc);
		if (other.doc < this.doc)
			return -1;
		else if (other.doc > this.doc)
			return 1;
		else
			return 0;
	}

}