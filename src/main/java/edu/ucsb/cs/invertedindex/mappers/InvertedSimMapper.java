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
 * @Since Aug 9, 2012
 */

package edu.ucsb.cs.invertedindex.mappers;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.ucsb.cs.types.DocDocWritable;
import edu.ucsb.cs.types.DocWeightArrayWritable;

/**
 * @author Maha
 * 
 */
public class InvertedSimMapper extends MapReduceBase implements
		Mapper<LongWritable, DocWeightArrayWritable, DocDocWritable, FloatWritable> {

	DocDocWritable outputkey = new DocDocWritable();
	FloatWritable partialSum = new FloatWritable();

	@Override
	public void configure(JobConf job) {
		super.configure(job);
	}

	public void map(LongWritable key, DocWeightArrayWritable value,
			OutputCollector<DocDocWritable, FloatWritable> output, Reporter reporter)
			throws IOException {
		for (int i = 0; i < value.vectorSize; i++)
			for (int j = i + 1; j < value.vectorSize; j++) {
				if (value.vector[i].docId < value.vector[j].docId) {
					outputkey.doc1 = value.vector[i].docId;
					outputkey.doc2 = value.vector[j].docId;
				} else {
					outputkey.doc2 = value.vector[i].docId;
					outputkey.doc1 = value.vector[j].docId;
				}
				partialSum.set(value.vector[i].weight * value.vector[j].weight);
				output.collect(outputkey, partialSum);
			}
	}
}
