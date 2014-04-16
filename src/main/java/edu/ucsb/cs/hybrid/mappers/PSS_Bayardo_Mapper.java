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
import java.util.BitSet;
import java.util.HashMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapred.OutputCollector;

import edu.ucsb.cs.types.DocDocWritable;
import edu.ucsb.cs.types.FeatureWeight;
import edu.ucsb.cs.types.IdFeatureWeightArrayWritable;
import edu.ucsb.cs.types.PostingDocWeight;

/**
 * Another version of #SingleS_Block0_Mapper for testing
 * Bayardo dynamic filtering [WWW'07] when "hybrid.google.skip" is set to true
 * Conclusion: useless
 */
public class PSS_Bayardo_Mapper extends PSS_Mapper {

	public BitSet skipSvec_i;// late
	float currentVecNorm1 = 0; // late

	@Override
	public void initialize(HashMap<Long, PostingDocWeight[]> split, boolean logV,
			boolean idCompareV, int splitSize) { // late
		super.initialize(split, logV, idCompareV, splitSize);
		skipSvec_i = new BitSet(splitSize);
	}

	@Override
	public void processVector(IdFeatureWeightArrayWritable currentVector) {

		int traverseSize, traverse, postingLen, sId;
		long minTerm;
		float oWeight;
		PostingDocWeight[] posting;
		FeatureWeight hold;
		skipSvec_i.clear();// late

		traverseSize = currentVector.vectorSize;
		long currentId = currentVector.id;
		currentVecNorm1 = currentVector.getNorm1(); // late
		for (traverse = 0; traverse < traverseSize; traverse++) {
			minTerm = (hold = currentVector.vector[traverse]).feature;
			posting = this.splitInvIndex.get(minTerm);
			if (posting == null)
				continue;
			oWeight = hold.weight;
			postingLen = posting.length;
			for (k = 0; k < postingLen; k++) {
				sId = posting[k].doc; // index i
				if (!skipSvec_i.get(sId))
					if (NotdynSkipThisS(sId)) // late
						if (idComparison) {
							if (checkMultiply(posting[k], currentId, oWeight))
								break; // skip rest of posting
						} else
							multiply(posting[k],currentId, oWeight);
			}
			currentVecNorm1 -= oWeight; // late
			if (skipSvec_i.nextClearBit(0) >= splitSize)
				return;
		}
	}

	public boolean NotdynSkipThisS(int id) {
		if ((accumulator[id] + currentVecNorm1 * maxwS[id]) < threshold) {
			skipSvec_i.set(id);
			return false;
		} else
			return true;
	}

	@Override
	public void flushAccumulator(OutputCollector<DocDocWritable, FloatWritable> out, long id)
			throws IOException {
		for (i = 0; i < splitSize; i++) {
			if (!skipSvec_i.get(i) && (th = accumulator[i]) >= this.threshold) {
				placeD.doc1 = this.IdMap[i];
				placeD.doc2 = id;
				placeF.set(th);
				out.collect(placeD, placeF);
			}
			accumulator[i] = 0.0f;
		}
	}

	public static void main(String[] args) {
		BitSet x = new BitSet(3);
		System.out.println("x size: " + x.size());
		System.out.println("x length: " + x.length());
		System.out.println("first clear bit:" + x.nextClearBit(0));
		System.out.println("first set bit:" + x.nextSetBit(0));
		x.set(1);
		System.out.println("first clear bit:" + x.nextClearBit(0));
		System.out.println("first set bit:" + x.nextSetBit(0));
		x.set(0);
		x.set(2);
		System.out.println("first clear bit:" + x.nextClearBit(0));
		System.out.println("first set bit:" + x.nextSetBit(0));
	}
}
