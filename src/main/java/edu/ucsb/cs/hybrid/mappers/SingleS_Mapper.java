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
import java.util.HashMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.ucsb.cs.hybrid.Config;
import edu.ucsb.cs.hybrid.io.Reader;
import edu.ucsb.cs.types.DocDocWritable;
import edu.ucsb.cs.types.PostingDocWeight;

public abstract class SingleS_Mapper extends MapReduceBase implements IMapper {

	/** inverted index of S with postings sorted increasingly by ID **/
	HashMap<Long, PostingDocWeight[]> splitInvIndex;
	float[] accumulator;
	float threshold, th;
	boolean log, idComparison;
	long[] IdMap;
	/* Google Dynamic */
	public float[] maxwS;

	/**
	 * SIGIR'14 parameters for PSS1, PSS2
	 * splitSize is s (may vary a little among maps)
	 * blockSize is b
	 */
	int i, j, k, splitSize, blockSize, filesNo;
	long opCount = 0, t, oA = 0, idskip = 0, idnotskip = 0;

	DocDocWritable placeD = new DocDocWritable();
	FloatWritable placeF = new FloatWritable();

	@Override
	public void configure(JobConf job) {
		blockSize = job.getInt(Config.COMP_BLOCK_PROPERTY, Config.COMP_BLOCK_VALUE);
		threshold = job.getFloat(Config.THRESHOLD_PROPERTY, Config.THRESHOLD_VALUE);
	}

	public void initialize(HashMap<Long, PostingDocWeight[]> split, boolean logV,
			boolean idCompareV, int splitSize) {
		this.splitSize = splitSize;
		splitInvIndex = split;
		allocateAccumulator();
		log = logV;
		idComparison = idCompareV;
	}

	public void allocateAccumulator() {
		accumulator = new float[splitSize];
	}

	public abstract void compareWith(Reader reader,
			OutputCollector<DocDocWritable, FloatWritable> output, Reporter reporter)
					throws IOException;

	@Override
	public void close() throws IOException {

		if (log) {
			System.out.println("splitSize s:" + splitSize);
			System.out.println("blockSize b:" + blockSize);
			if (idComparison) {
				System.out.println("skipped multiplication from id comoparison: " + idskip);
				System.out.println("performed multiplication with id comoparison: " + idnotskip);
			}
			System.out.println("OperationCount:" + opCount + "\n");
			System.out.println("output time in millisec:" + oA / 1000000.0);
		}
		return;
	}

	public void flushAccumulator(OutputCollector<DocDocWritable, FloatWritable> out, long id)
			throws IOException {
		for (i = 0; i < splitSize; i++) {
			if ((th = accumulator[i]) >= this.threshold) {
				placeD.doc1 = this.IdMap[i];
				placeD.doc2 = id;
				if(placeD.doc1 != placeD.doc2){
					placeF.set(th);
					out.collect(placeD, placeF);
				}
			}
			accumulator[i] = 0.0f;
		}
	}
}
