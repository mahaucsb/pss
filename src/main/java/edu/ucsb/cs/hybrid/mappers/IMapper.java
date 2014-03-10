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
 * @Since Dec 6, 2012
 */

package edu.ucsb.cs.hybrid.mappers;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.ucsb.cs.hybrid.io.Reader;
import edu.ucsb.cs.types.DocDocWritable;
import edu.ucsb.cs.types.FeatureWeightArrayWritable;
import edu.ucsb.cs.types.PostingDocWeight;

/**
 * Interface for the PSS mappers. This is to have one runner instead of two for Single vs.
 *         Multiple S ,but not yet done.
 * @author Maha 
 */
public interface IMapper extends
		Mapper<LongWritable, FeatureWeightArrayWritable, DocDocWritable, FloatWritable> {
	

	public void configure(JobConf job);

	public void initialize(HashMap<Long, PostingDocWeight[]> split, boolean logV,
			boolean idCompareV, int splitSize);

	public void allocateAccumulator();

	public abstract void compareWith(Reader reader,
			OutputCollector<DocDocWritable, FloatWritable> output, Reporter reporter)
			throws IOException;

	public void close() throws IOException;

	public void flushAccumulator(OutputCollector<DocDocWritable, FloatWritable> out, long id)
			throws IOException;
}
