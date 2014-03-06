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
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.ucsb.cs.hybrid.Config;
import edu.ucsb.cs.hybrid.io.Reader;
import edu.ucsb.cs.types.DocDocWritable;
import edu.ucsb.cs.types.IdFeatureWeightArrayWritable;
import edu.ucsb.cs.types.IndexFeatureWeight;

/**
 * Modified version of Block-1 to have b processed by splitting it into mini-b,
 * hence the accumulator is of size mini_s * mini_b re-used sequentially.
 */
public class MultipleS_Block11_Mapper extends MultipleS_Block1_Mapper {

	int comp_b;

	@Override
	public void configure(JobConf job) {
		comp_b = job.getInt(Config.COMP_BLOCK_PROPERTY, Config.COMP_BLOCK_VALUE);
		super.configure(job);
	}

	@Override
	public void compareWith(Reader reader, OutputCollector<DocDocWritable, FloatWritable> output,
			Reporter reporter) throws IOException {
		Boolean fileNotEmpy = true;
		IdFeatureWeightArrayWritable[] block;
		int[] currentBpointers = new int[blockSize];
		int bSize, recordNo;
		IdFeatureWeightArrayWritable currentRecord;
		long[] IdMap;

		while (fileNotEmpy) {
			block = reader.getNextbVectors(blockSize);
			bSize = reader.nbVectors;
			if (bSize == 0)
				break;
			for (currentS = 0; currentS < nSplits; currentS++) {
				initCurrentB(block, bSize);
				IdMap = this.IdMaps.get(currentS);
				while (updateCurrentB(block, bSize)) {
					processOneFeature(currentB, currentS, block, IdMap);
				}
				if (log) {
					t = System.nanoTime();
					flushAccumulator(output, block, bSize, IdMap);
					oA += (System.nanoTime() - t);
				} else
					flushAccumulator(output, block, bSize, IdMap);
			}
		}
	}

	@Override
	public void allocateCurrentB(IndexFeatureWeight[] currentB, int bSize) {
		this.currentB = new IndexFeatureWeight[comp_b];
		for (i = 0; i < comp_b; i++)
			this.currentB[i] = new IndexFeatureWeight(0, Long.MAX_VALUE, 0);
		currentBpointers = new int[comp_b];
	}

	@Override
	public void allocateAccumulator() {
		accumulator = new float[splitSize][comp_b];
	}
}
