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

package edu.ucsb.cs.hybrid.mappers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapred.OutputCollector;

import edu.ucsb.cs.types.DocDocWritable;
import edu.ucsb.cs.types.PostingDocWeight;

public abstract class MultipleS_Mapper extends SingleS_Mapper {

	ArrayList<HashMap<Long, PostingDocWeight[]>> splitInvIndexes;
	ArrayList<long[]> IdMaps; 
	int currentS, nSplits;

	public void initialize(ArrayList<HashMap<Long, PostingDocWeight[]>> splits, boolean logV,
			boolean idCompareV, int splitsSize) {
		splitSize = splitsSize;
		splitInvIndexes = splits;
		nSplits = splitInvIndexes.size();
		allocateAccumulator();
		log = logV;
		idComparison = idCompareV;
	}

	@Override
	public void flushAccumulator(OutputCollector<DocDocWritable, FloatWritable> out, long id)
			throws IOException {
		for (i = 0; i < splitSize; i++) {
			if ((th = accumulator[i]) >= this.threshold) {
				long[] list = this.IdMaps.get(currentS);
				placeD.doc1 = list[i];
				placeD.doc2 = id;
				if(placeD.doc1!=placeD.doc2){
				placeF.set(th);
				out.collect(placeD, placeF);}
			}
			accumulator[i] = 0.0f;
		}
	}
}
