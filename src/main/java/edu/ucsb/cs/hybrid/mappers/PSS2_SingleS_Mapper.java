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
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.ucsb.cs.hybrid.Config;
import edu.ucsb.cs.hybrid.io.Reader;
import edu.ucsb.cs.types.DocDocWritable;
import edu.ucsb.cs.types.FeatureWeightArrayWritable;
import edu.ucsb.cs.types.IdFeatureWeightArrayWritable;
import edu.ucsb.cs.types.IndexFeatureWeight;
import edu.ucsb.cs.types.PostingDocWeight;

/**
 * PSS2 without multiple splits in S and ioB=compB.
 * blocks features from B vectors then perform the comparison with S.
 */
public class PSS2_SingleS_Mapper extends SingleS_Mapper {

	float[][] accumulator;
	IndexFeatureWeight[] currentB;
	int[] currentBpointers;
	int bfeatureSharingNum;
	boolean loopsloopb;

	public static final String LOOPSLOOPB_PROPERTY = Config.NAMESPACE + ".loops.loopb";
	public static final boolean LOOPSLOOPB_VALUE = true;

	@Override
	public void configure(JobConf job) {
		super.configure(job); // idComparison set ?
		loopsloopb = job.getBoolean(LOOPSLOOPB_PROPERTY, LOOPSLOOPB_VALUE);
		currentB = new IndexFeatureWeight[blockSize];
		for (i = 0; i < blockSize; i++)
			currentB[i] = new IndexFeatureWeight(0, Long.MAX_VALUE, 0);
		currentBpointers = new int[blockSize];
	}

	@Override
	public void allocateAccumulator() {
		accumulator = new float[splitSize][blockSize];
	}

	public void initCurrentB(IdFeatureWeightArrayWritable[] block, int bSize)
			throws UnsupportedEncodingException {
		for (i = 0; i < bSize; i++) {
			try {
				currentB[i].set(i, block[i].getFeature(0), block[i].getWeight(0));
			} catch (ArrayIndexOutOfBoundsException e) {}
			currentBpointers[i] = 0;
		}
		bfeatureSharingNum = 0;
	}

	public boolean updateCurrentB(IdFeatureWeightArrayWritable[] block, int bSize) {
		for (i = 0; i < bfeatureSharingNum; i++) {
			int cb = currentB[i].index;
			currentBpointers[cb]++;
			try {
				currentB[i].setFeatureWeight(block[cb].getFeature(currentBpointers[cb]),
						block[cb].getWeight(currentBpointers[cb]));
			} catch (Exception e) {
				currentB[i].setFeatureWeight(Long.MAX_VALUE, -1);
			}
		}
		Arrays.sort(currentB);
		if (currentB[0].weight == -1)
			return false; // block all processed
		i = -1;
		while ((++i < (bSize - 1)) && (currentB[i].feature == currentB[i + 1].feature)) {}
		bfeatureSharingNum = (i + 1); // number of vectors in B that share min feature
		return true;
	}

	public void processOneFeature(IndexFeatureWeight[] currentB,
			IdFeatureWeightArrayWritable[] block) {

		long feature = currentB[0].feature;
		PostingDocWeight[] posting = this.splitInvIndex.get(feature);
		if (posting == null)
			return;
		//
		// Loop Order Analysis
		//
		if (loopsloopb) {
			for (j = 0; j < posting.length; j++) {
				int cs = posting[j].doc;
				float sWeight = posting[j].weight;
				for (i = 0; i < bfeatureSharingNum; i++) {
					int cb = currentB[i].index;
					multiplyB(accumulator[cs], cs, sWeight, cb, currentB[i].weight, block);
				}
			}
		} else {
			for (i = 0; i < bfeatureSharingNum; i++) {
				float oWeight = currentB[i].weight;
				int cb = currentB[bfeatureSharingNum].index;
				for (j = 0; j < posting.length; j++) {
					multiplyS(posting[j], cb, oWeight, block);
				}
			}
		}
	}

	@Override
	public void compareWith(Reader reader, OutputCollector<DocDocWritable, FloatWritable> output,
			Reporter reporter) throws IOException {

		Boolean fileNotEmpy = true;
		IdFeatureWeightArrayWritable[] block;
		int[] currentBpointers = new int[blockSize];
		int bSize, recordNo;
		IdFeatureWeightArrayWritable currentRecord;

		while (fileNotEmpy) {
			block = reader.getNextbVectors(blockSize);
			bSize = reader.nbVectors;
			if (bSize == 0)
				break;
			initCurrentB(block, bSize);
			while (updateCurrentB(block, bSize)) {
				processOneFeature(currentB, block);
			}
			if (log) {
				t = System.nanoTime();
				flushAccumulator(output, block, bSize);
				oA += (System.nanoTime() - t);
			} else
				flushAccumulator(output, block, bSize);
		}
	}

	public void multiplyS(PostingDocWeight postingS, int cb, float bWeight,
			IdFeatureWeightArrayWritable[] block) {
		// opCount++;
		if (idComparison) {
			if (this.IdMap[postingS.doc] < block[cb].id)
				accumulator[postingS.doc][cb] += (postingS.weight * bWeight);
		} else
			accumulator[postingS.doc][cb] += (postingS.weight * bWeight);
	}

	/**
	 * @param accumS: accumulator of size b for a particual s.
	 * @param sWeight: weight for the current feature processed
	 * @param cb
	 * @param bWeight
	 */
	public void multiplyB(float[] accumS, int cs, float sWeight, int cb, float bWeight,
			IdFeatureWeightArrayWritable[] block) {
		// opCount++;
		if (idComparison) {
			if (this.IdMap[cs] < block[cb].id)
				accumS[cb] += (sWeight * bWeight);
		} else
			accumS[cb] += (sWeight * bWeight);
	}

	public void flushAccumulator(OutputCollector<DocDocWritable, FloatWritable> out,
			IdFeatureWeightArrayWritable[] block, int bSize) throws IOException {
		for (i = 0; i < splitSize; i++) {
			float[] oneS = accumulator[i];
			for (j = 0; j < bSize; j++) {
				if ((th = oneS[j]) >= this.threshold) {
					placeD.doc1 = this.IdMap[i];
					placeD.doc2 = block[j].id;
					if(placeD.doc1!=placeD.doc2){
					placeF.set(th);
					out.collect(placeD, placeF);}
				}
				oneS[j] = 0.0f;
			}
		}
	}

	public void map(LongWritable key, FeatureWeightArrayWritable value,
			OutputCollector<DocDocWritable, FloatWritable> output, Reporter reporter)
			throws IOException {}
}
