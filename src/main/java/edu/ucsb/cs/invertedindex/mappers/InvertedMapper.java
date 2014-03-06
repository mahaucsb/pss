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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.ucsb.cs.types.DocWeight;
import edu.ucsb.cs.types.FeatureWeight;
import edu.ucsb.cs.types.FeatureWeightArrayWritable;

/**
 * @author Maha
 * 
 */
public class InvertedMapper extends MapReduceBase implements
		Mapper<LongWritable, FeatureWeightArrayWritable, LongWritable, DocWeight> {

	LongWritable feature = new LongWritable();
	DocWeight post = new DocWeight();

	@Override
	public void configure(JobConf job) {
		super.configure(job);
	}

	public void map(LongWritable key, FeatureWeightArrayWritable value,
			OutputCollector<LongWritable, DocWeight> output, Reporter reporter) throws IOException {
		post.docId = key.get();
		for (FeatureWeight item : value.vector) {
			post.weight = item.weight;
			feature.set(item.feature);
			output.collect(feature, post);
		}
	}
}
