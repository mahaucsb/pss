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
import org.apache.hadoop.mapred.Reporter;

import edu.ucsb.cs.hybrid.io.Reader;
import edu.ucsb.cs.types.DocDocWritable;
import edu.ucsb.cs.types.FeatureWeight;
import edu.ucsb.cs.types.IdFeatureWeightArrayWritable;
import edu.ucsb.cs.types.PostingDocWeight;

/*
 * Tested looks good.
 */
public class PSS1_Threaded_Mapper extends PSS1_Mapper {

	MapperThread[] threads;
	Boolean keepWaiting = true;

	@Override
	public void initialize(ArrayList<HashMap<Long, PostingDocWeight[]>> splits, boolean logV,
			boolean idCompareV, int splitsSize) {
		super.initialize(splits, logV, idCompareV, splitsSize);
		initThreads();
	}

	@Override
	public void allocateAccumulator() {}

	public void initThreads() {
		threads = new MapperThread[nSplits];
		for (i = 0; i < nSplits; i++) {
			threads[i] = new MapperThread(i, keepWaiting);
			threads[i].start();
		}
	}

	@Override
	public void compareWith(Reader reader, OutputCollector<DocDocWritable, FloatWritable> output,
			Reporter reporter) throws IOException {

		for (i = 0; i < nSplits; i++)
			threads[i].setOutputCollector(output);

		IdFeatureWeightArrayWritable[] block;
		int bSize;

		while (true) {
			block = reader.getNextbVectors(blockSize);
			bSize = reader.nbVectors;
			if (bSize == 0)
				break;
			passBlock(nSplits, block, bSize);
			synchronized (keepWaiting) {
				keepWaiting.notifyAll();
			}
			while (!threadsAllWaiting(nSplits))
				Thread.yield();
			// threads are all waiting for next b to be read my main
		}
		// current file is all processed
	}

	synchronized public void passBlock(int n, IdFeatureWeightArrayWritable[] block, int bSize) {
		for (int i = 0; i < n; i++)
			threads[i].setBlock(block, bSize);
	}

	synchronized public boolean threadsAllWaiting(int n) {
		for (int i = 0; i < n; i++) {
			if (!threads[i].getState().equals(Thread.State.WAITING))
				return false;
		}
		return true;
	}

	// //////////////////////////// THREADS ZONE //////////////////////////////
	/**
	 * Each thread responsible for a split in S. It works till all B is
	 * processed then waits for the next B to be read by parent thread.
	 */
	class MapperThread extends Thread {
		Boolean keepWaiting;

		int tid, bSize;
		HashMap<Long, PostingDocWeight[]> threadSIndex;
		long[] threadSId;
		float[] threadAccum = new float[splitSize];
		IdFeatureWeightArrayWritable[] block;
		OutputCollector<DocDocWritable, FloatWritable> out;

		MapperThread(int tid, Boolean keepWaiting) {
			this.tid = tid;
			threadSIndex = splitInvIndexes.get(tid);
			threadSId = IdMaps.get(this.tid);
			this.keepWaiting = keepWaiting;
		}

		void setOutputCollector(OutputCollector<DocDocWritable, FloatWritable> out) {
			this.out = out;
		}

		public void stopWaiting() {
			this.keepWaiting = false;
		}

		public void setBlock(IdFeatureWeightArrayWritable[] block, int bSize) {
			this.block = block;
			this.bSize = bSize;
		}

		@Override
		synchronized public void run() {
			while (keepWaiting) {
				synchronized (keepWaiting) {
					try {
						keepWaiting.wait();
					} catch (InterruptedException e) {}
				}
				this.processBlock();
			}
		}

		void processBlock() {
			int recordNo, traverse, postingLen;
			long currentId, minTerm;
			float oWeight;
			PostingDocWeight[] posting = null;
			FeatureWeight hold;
			IdFeatureWeightArrayWritable currentBRecord = null;
			for (recordNo = 0; recordNo < bSize; recordNo++) {
				currentBRecord = block[recordNo];
				currentId = currentBRecord.id;
				for (traverse = 0; traverse < currentBRecord.vectorSize; traverse++) {
					minTerm = (hold = currentBRecord.vector[traverse]).feature;
					posting = this.threadSIndex.get(minTerm);
					if (posting == null) {
						continue;
					}
					oWeight = hold.weight;
					postingLen = posting.length;
					for (int k = 0; k < postingLen; k++)
						if (idComparison) {
							if (checkMultiply(posting[k], currentId, oWeight))
								break;
						} else
							multiply(posting[k],currentId, oWeight);
				}
				if (log) {
					t = System.nanoTime();
					flushThreadAccumulator(currentId);
					oA += (System.nanoTime() - t);
				} else
					flushThreadAccumulator(currentId);
			}
		}

		public void multiply(PostingDocWeight postingK,long oId, float oWeight) {
			// opCount++; //this is global should be synch
			int kId = postingK.doc;
			if (IdMap[kId] == oId) return;
			threadAccum[kId] += (postingK.weight * oWeight);
		}

		public boolean checkMultiply(PostingDocWeight postingK, long oId, float oWeight) {
			int kId = postingK.doc;
			if (this.threadSId[kId] < oId) {
				multiply(postingK,oId, oWeight);
				return false;
			} else
				return true;
		}

		public void flushThreadAccumulator(long bid) {
			for (int i = 0; i < splitSize; i++) {
				if ((th = threadAccum[i]) >= threshold) {
					placeD.doc1 = this.threadSId[i];
					placeD.doc2 = bid;
					placeF.set(th);
					try {
						out.collect(placeD, placeF);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				threadAccum[i] = 0.0f;
			}
		}
	}
}
