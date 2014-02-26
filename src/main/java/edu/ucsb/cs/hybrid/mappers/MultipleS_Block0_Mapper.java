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
 * BUG: oneMap(5) split into (2 or 3) and others (10)
 */
public class MultipleS_Block0_Mapper extends MultipleS_HybridMapper {
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
				processRecord(currentRecord, output);
			}
		}
	}

	public void processRecord(IdFeatureWeightArrayWritable currentRecord,
			OutputCollector<DocDocWritable, FloatWritable> output) throws IOException {

		int traverseSize, traverse, postingLen;
		long minTerm;
		float oWeight;
		PostingDocWeight[] posting;
		FeatureWeight hold;

		traverseSize = currentRecord.vectorSize;
		long currentId = currentRecord.id;

		for (currentS = 0; currentS < nSplits; currentS++) {
			for (traverse = 0; traverse < traverseSize; traverse++) {
				minTerm = (hold = currentRecord.vector[traverse]).feature;
				posting = this.splitInvIndexes.get(currentS).get(minTerm);
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
			if (log) {
				t = System.nanoTime();
				flushAccumulator(output, currentRecord.id);
				oA += (System.nanoTime() - t);
			} else
				flushAccumulator(output, currentRecord.id);
		}
	}

	/**
	 * {@link SingleS_Block0_Mapper#multiply(PostingDocWeight, float)}
	 */
	public void multiply(PostingDocWeight postingK, float oWeight) {
		opCount++;
		int kId = postingK.doc;
		accumulator[kId] += (postingK.weight * oWeight);
	}

	/**
	 * {@link SingleS_Block0_Mapper#checkMultiply(PostingDocWeight, long, float)}
	 */
	public boolean checkMultiply(PostingDocWeight postingK, long oId, float oWeight) {
		int kId = postingK.doc;
		if (IdMaps.get(currentS)[kId] < oId) {
			multiply(postingK, oWeight);
			// accumulator[kId] += (postingK.weight * oWeight);
			return false;
		} else
			return true;
	}

	public void map(LongWritable key, FeatureWeightArrayWritable value,
			OutputCollector<DocDocWritable, FloatWritable> output, Reporter reporter)
			throws IOException {}
}
