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
import java.util.BitSet;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.ucsb.cs.hybrid.io.Reader;
import edu.ucsb.cs.types.DocDocWritable;
import edu.ucsb.cs.types.IdFeatureWeightArrayWritable;
import edu.ucsb.cs.types.PostingDocWeight;

/**
 * PSS3 is a third implementation but didn't show any imporovmenet. Need explanation!
 * not tested yet
 */
public class PSS3_SingleS_Mapper extends PSS2_SingleS_Mapper {

	int r = 32, range = 300;
	long count = 0;
	ArrayList2dstatic<PostingDocWeight> Spointers = new ArrayList2dstatic<PostingDocWeight>();
	ArrayList2d<PostingDocWeight> Bpointers = new ArrayList2d<PostingDocWeight>();
	int[] sCurrentIndex = new int[r];
	BitSet Bdone;
	int[] BdocumentLen;
	boolean NominTerm;

	IndexTermWeight[] blockCurrent;

	@Override
	public void configure(JobConf job) {
		super.configure(job);
		PostingDocWeight[] b = new PostingDocWeight[1];
		for (i = 0; i < r; i++)
			Spointers.addCol(b);
		ArrayList<PostingDocWeight> a = new ArrayList<PostingDocWeight>();
		for (i = 0; i < r; i++)
			Bpointers.addRow(a);
		this.range = Integer.parseInt(job.get("RANGE"));
		this.r = Integer.parseInt(job.get("R"));
		Bdone = new BitSet(blockSize);
		blockCurrent = new IndexTermWeight[blockSize];
		for (i = 0; i < blockSize; i++)
			blockCurrent[i] = new IndexTermWeight();
		BdocumentLen = new int[blockSize];
	}

	public ArrayList<PostingDocWeight> getrMinDocs(IdFeatureWeightArrayWritable[] block,
			Boolean NullValue, int Blen, long min, int rCount) {
		IndexTermWeight point;
		ArrayList<PostingDocWeight> idWeight = new ArrayList<PostingDocWeight>();
		if (!NullValue) {
			for (i = 0; i < Blen; i++)
				if (!(Bdone.get(i)) && (min == this.blockCurrent[i].term)) {
					point = blockCurrent[i];
					idWeight.add(new PostingDocWeight(i, point.weight));
					if (point.index < BdocumentLen[i] - 1) {
						point.index++;
						point.term = block[i].vector[point.index].feature;
						point.weight = block[i].vector[point.index].weight;
					} else
						Bdone.set(i);
				}
		} else {
			for (i = 0; i < Blen; i++)
				if (!(Bdone.get(i)) && (min == this.blockCurrent[i].term)) {
					point = blockCurrent[i];
					if (point.index < BdocumentLen[i] - 1) {
						point.index++;
						point.term = block[i].vector[point.index].feature;
						point.weight = block[i].vector[point.index].weight;
					} else
						Bdone.set(i);
				}
		}
		return idWeight; // <0,0.1><2,0.03>..
	}

	public long getMinTerm(IdFeatureWeightArrayWritable[] block, int Blen) {
		int i;
		NominTerm = true;
		long point;
		long min = -1;
		for (i = 0; i < Blen; i++)
			if (!Bdone.get(i)) {
				min = blockCurrent[i].term;
				NominTerm = false;
				break;
			}
		if (!NominTerm)
			for (i = 0; i < Blen; i++) {
				if (!Bdone.get(i))
					if (min > (point = blockCurrent[i].term))
						min = point;
			}
		return min;
	}

	// // S to B comparison
	// public void myMapPhase2(int fileNo, OutputCollector<DocDocWritable,
	// FloatWritable> output,
	// Reporter reporter) throws IOException {
	@Override
	public void compareWith(Reader reader, OutputCollector<DocDocWritable, FloatWritable> output,
			Reporter reporter) throws IOException {
		Boolean fileNotEmpy = true;
		IdFeatureWeightArrayWritable[] block;

		int sDocid, PostLen, k, BLen, numMinDocs, rCount;
		ArrayList<PostingDocWeight> MinDocWeight;
		long minTerm = 0;
		float sfreq;
		float[] acc;
		PostingDocWeight[] posting;
		PostingDocWeight postingK;
		PostingDocWeight Bdoc;
		BitSet SpostingsDone = new BitSet(r);
		boolean goNext;

		while (true) {// Read blockSize of records from fileNo until empty
			block = reader.getNextbVectors(blockSize);
			BLen = reader.nbVectors;
			if (BLen == 0)
				break;
			initCurrentB(block, BLen);

			while (true) {// for all B terms
				rCount = 0;
				while (rCount < r) {
					minTerm = getMinTerm(block, BLen);
					if (NominTerm)
						break;
					posting = this.splitInvIndex.get(minTerm);
					if (posting == null)
						goNext = true;
					else
						goNext = false;
					MinDocWeight = getrMinDocs(block, goNext, BLen, minTerm, rCount);
					if (goNext)
						continue;
					Spointers.setCol(rCount, posting);
					Bpointers.setCol(rCount, MinDocWeight);
					rCount++;
				}
				if (rCount == 0)
					break;

				SpostingsDone.clear();

				for (i = 0; i < rCount; i++)
					sCurrentIndex[i] = 0;
				while (!SDone(SpostingsDone, rCount)) {
					for (j = 0; j < rCount; j++) {
						if (SpostingsDone.get(j))
							continue;
						MinDocWeight = Bpointers.getRow(j);
						numMinDocs = Bpointers.getRowSize(j); // B posting of
						// term j
						posting = Spointers.getCol(j);
						PostLen = posting.length;

						for (k = sCurrentIndex[j]; k < PostLen; k++) {
							postingK = posting[k];
							sDocid = postingK.doc;
							if (sDocid > range)
								break; // go to next term posting
							sfreq = postingK.weight;
							acc = accumulator[sDocid];
							for (i = 0; i < numMinDocs; i++) {
								Bdoc = MinDocWeight.get(i);
								// if(IndexMap[sDocid] < Bdoc.doc) {
								count++;
								acc[Bdoc.doc] += (sfreq * Bdoc.weight);
								// }
							}
						}
						// p("Multiplication count = "+count+"\n");
						if (k >= PostLen)
							SpostingsDone.set(j);
						else {
							range += range;
							sCurrentIndex[j] = k;
						}
					}
				}
				// ///cc+=(System.nanoTime()-tcc);
			}

			if (log) { /*cA+=(System.nanoTime()-t);*/
				t = System.nanoTime();
			}
			Bdone.clear();
			// flushBMap(output,BLen); //flush map after one block is done
			if (log)
				oA += (System.nanoTime() - t);
			if (BLen < blockSize)
				break;
			// if(log) Other+=(System.nanoTime()-ti);
		} // Read next B records from Other Partition
	}

	// True if all bits are set
	public boolean SDone(BitSet SpostingsDone, int rCount) {
		for (i = 0; i < rCount; i++)
			if (!SpostingsDone.get(i))
				return false;
		return true;
	}

	@Override
	public void close() throws IOException {}

	/**************************************************************
	 * FLUSH HASHMAP TO DISK
	 **************************************************************/

	public void flushBMap(IdFeatureWeightArrayWritable[] block,
			OutputCollector<DocDocWritable, FloatWritable> o, int BlockLen) throws IOException {
		// flushCount++;
		int s;
		float th;
		float[] scores;
		for (i = 0; i < blockSize; i++) {
			scores = accumulator[i];
			for (j = 0; j < BlockLen; j++) {
				if ((th = scores[j]) > this.threshold) {
					placeD.doc1 = this.IdMap[i]; // Original Id
					placeD.doc2 = block[j].id;
					if(placeD.doc1!=placeD.doc2){
					placeF.set(th);
					o.collect(placeD, placeF);}
				}
				scores[j] = 0.0f;
			}
		}
		Bdone.clear();
	}

}

class ArrayList2dstatic<Type> {
	ArrayList<Type[]> array;
	int ColsNum = 0;

	public ArrayList2dstatic() {
		array = new ArrayList<Type[]>();
	}

	public ArrayList2dstatic(int r, Type[] a) {
		try {
			int i;
			for (i = 0; i < r; i++)
				array.add(a);
			ColsNum = r;
			System.out.println("r:" + r + ",ColsNum:" + ColsNum + "\n");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void addCol(Type[] a) {
		ColsNum++;
		array.add(a);
	}

	public void clearCols() {
		int i;
		for (i = 0; i < ColsNum; i++)
			array.remove(0);
		ColsNum = 0;
	}

	public Type getColRow(int col, int row) {
		if ((col < ColsNum) && (row < getColSize(col)))
			return array.get(col)[row];
		else
			return null;
	}

	public void setCol(int col, Type[] a) {
		// array.set(col, a);/*
		if (col < ColsNum)
			array.set(col, a);
		else {
			System.out.println("col:" + col + ",NumCol:" + ColsNum + "\n");
			throw new IndexOutOfBoundsException();
		}
	}

	public int getNumCols() {
		return ColsNum;
	}

	public int getColSize(int i) {
		return array.get(i).length;
	}

	public Type[] getCol(int i) {
		return array.get(i);
	}

	public void print() {
		int i, j;
		for (i = 0; i < array.size(); i++) {
			for (j = 0; j < array.get(i).length; j++)
				System.out.print(array.get(i)[j] + " ");
			System.out.println();
		}
	}
}

class ArrayList2d<Type> {
	ArrayList<ArrayList<Type>> array;
	int currentRow = 0;

	public ArrayList2d() {
		array = new ArrayList<ArrayList<Type>>();
	}

	public ArrayList2d(int r, ArrayList<Type> hold) {
		int i;
		for (i = 0; i < r; i++)
			array.add(hold);
	}

	public void setCol(int i, ArrayList<Type> a) {
		array.set(i, a);
	}

	public void print() {
		int i, j;
		for (i = 0; i < array.size(); i++) {
			for (j = 0; j < array.get(i).size(); j++)
				System.out.print(array.get(i).get(j) + " ");
			System.out.println();
		}
	}

	public void addRow(ArrayList<Type> a) {
		if (array.add(a))
			currentRow++;
	}

	public void clearRows() {
		int i;
		for (i = 0; i < currentRow; i++)
			array.remove(0);
		currentRow = 0;
	}

	public int getNumRows() {
		return array.size();
	}

	public int getRowSize(int i) {
		return array.get(i).size();
	}

	public ArrayList<Type> getRow(int i) {
		return array.get(i);
	}
}

class IndexTermWeight {
	public int index;
	public long term;
	public float weight;

	IndexTermWeight() {};

	IndexTermWeight(int i, long t, float w) {
		index = 1;
		term = 1;
		weight = w;
	}

	public void set(int i, long t, float w) {
		index = 1;
		term = 1;
		weight = w;
	}
}
