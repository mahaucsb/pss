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

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.ucsb.cs.hybrid.io.Reader;
import edu.ucsb.cs.hybrid.types.DocDocWritable;
import edu.ucsb.cs.hybrid.types.FeatureWeight;
import edu.ucsb.cs.hybrid.types.FeatureWeightArrayWritable;
import edu.ucsb.cs.hybrid.types.IdFeatureWeightArrayWritable;
import edu.ucsb.cs.hybrid.types.PostingDocWeight;

/*
 * limitation: metwally is only here
 *  - emphasize that positional/size based filtering are hard for weighted cosine. plus not 
 *  applicable here because we compare S vs. 1 which reduce effectiveness. 
 *
 * Change it to embed:
 *  - baraglia dummy not only max-weight of each feature but if that maxWeight appeared multiple times. 
 *  this is by saving top 5 maxWeights with count, then pick largest with count >=2.
 * 	- baraglia : each record is <# of pruned> <maxW-pruned> <prune> <indexed>
 *  - after indexed part is done, if flag is set check : maxW-pruned * norm1-remaining(S) < th skip
 *  - sort postings based on norm1 decreasingly. (don't know if worth using still).
 *  - else go over pruned. 
 */
public class SingleS_Block0_Mapper extends SingleS_HybridMapper {

	@Override
	public void compareWith(Reader reader, OutputCollector<DocDocWritable, FloatWritable> output,
			Reporter reporter) throws IOException {
		Boolean fileNotEmpy = true;
		IdFeatureWeightArrayWritable[] block;
		int bSize, recordNo;
		IdFeatureWeightArrayWritable currentRecord;
		while (fileNotEmpy) {
			block = reader.getNextbVectors(blockSize);
			bSize = reader.nbVectors;
			if (bSize == 0)
				break;

			for (recordNo = 0; recordNo < bSize; recordNo++) {
				currentRecord = block[recordNo];
				processVector(currentRecord);
				if (log) {
					t = System.nanoTime();
					flushAccumulator(output, currentRecord.id); 
					oA += (System.nanoTime() - t);
				} else{
				    flushAccumulator(output, currentRecord.id); 
				}
			}
		}
	}

	public void processVector(IdFeatureWeightArrayWritable currentRecord) {

		int traverseSize, traverse, postingLen;
		long minTerm;
		float oWeight;
		PostingDocWeight[] posting;
		FeatureWeight hold;

		traverseSize = currentRecord.vectorSize;
		long currentId = currentRecord.id;
		for (traverse = 0; traverse < traverseSize; traverse++) {
			minTerm = (hold = currentRecord.vector[traverse]).feature;
			posting = this.splitInvIndex.get(minTerm);// posting of minTerm
			if (posting == null)
				continue;
			oWeight = hold.weight;
			postingLen = posting.length;
			for (k = 0; k < postingLen; k++)
				if (idComparison) {
					if (checkMultiply(posting[k], currentId, oWeight))
						break;
				} else
					multiply(posting[k], oWeight);
		}
	}

	/**
	 * Used when circular load balancing is enabled.
	 * @param postingK : current (v,w) in posting of the feature inspected.
	 * @param oWeight : weight of the feature from the vector in B.
	 */
	public void multiply(PostingDocWeight postingK, float oWeight) {
		opCount++;
		int kId = postingK.doc;
		accumulator[kId] += (postingK.weight * oWeight);
	}

	/**
	 * Used when circular load balancing is disabled.
	 * @param postingK : current (v,w) in posting of the feature inspected.
	 * @param oId : ID of the vector in B.
	 * @param oWeight: weight of the feature from the vector in B.
	 * @return true means skip rest of posting for block-0, else partial score
	 *         is added to accumulator.
	 */
	public boolean checkMultiply(PostingDocWeight postingK, long oId, float oWeight) {
		int kId = postingK.doc;
		if (IdMap[kId] < oId) {
			multiply(postingK, oWeight);
			return false;
		} else
			return true;
	}

	public void map(LongWritable key, FeatureWeightArrayWritable value,
			OutputCollector<DocDocWritable, FloatWritable> output, Reporter reporter)
			throws IOException {}
}
