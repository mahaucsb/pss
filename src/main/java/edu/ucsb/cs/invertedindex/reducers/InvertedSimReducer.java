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

package edu.ucsb.cs.invertedindex.reducers;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.ucsb.cs.types.DocDocWritable;

/**
 * @author Maha
 * 
 */
public class InvertedSimReducer extends MapReduceBase implements
		Reducer<DocDocWritable, FloatWritable, DocDocWritable, FloatWritable> {

	DocDocWritable docdoc = new DocDocWritable();
	FloatWritable outsum = new FloatWritable();

	public void reduce(DocDocWritable key, Iterator<FloatWritable> values,
			OutputCollector<DocDocWritable, FloatWritable> output, Reporter reporter)
			throws IOException {
		float sum = 0;
		while (values.hasNext()) {
			FloatWritable s = values.next();
			sum += s.get();
			if (sum > 0.9) {
				docdoc.doc1 = key.doc1;
				docdoc.doc2 = key.doc2;
				outsum.set(sum);
				output.collect(docdoc, outsum);
			}
		}
	}
}