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

package edu.ucsb.cs.bruteforce;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;

import edu.ucsb.cs.lsh.projection.RandomVector;
import edu.ucsb.cs.types.DocDocWritable;
import edu.ucsb.cs.types.FeatureWeight;
import edu.ucsb.cs.types.FeatureWeightArrayWritable;

public abstract class ForwardMapper extends MapReduceBase implements
		Mapper<LongWritable, FeatureWeightArrayWritable, DocDocWritable, FloatWritable> {

	/**
	 * @param v1
	 * @param v2
	 * @return dot product of (v1 and v2) stops as soon as threshold is
	 *         exceeded. assumes vectors are sorted by feature.
	 */
	public static boolean dotLessThan(FeatureWeightArrayWritable v1, FeatureWeightArrayWritable v2,
			float threshold) {

		int i = 0, j = 0;
		float product = 0;
		FeatureWeight[] vec1 = v1.sortById();
		FeatureWeight[] vec2 = v2.sortById();

		while (product < threshold && i < vec1.length && j < vec2.length)
			if (vec1[i].feature < vec2[j].feature)
				i++;
			else if (vec1[i].feature > vec2[j].feature)
				j++;
			else {
				product += (vec1[i].weight * vec2[j].weight);
				i++;
				j++;
			}
		if (product < threshold)
			return true;
		else
			return false;
	}

	/**
	 * @return dot product of (v1 and v2) . Assumes features to be hashed
	 *         sequentially (not MD5) and sorted following a serial order.
	 *         Notice, the result can be less or greater than zero since vectors
	 *         are normalized with some negative weights.
	 */

	public static float projectionDot(FeatureWeightArrayWritable v1, RandomVector v2) {

		if (v1.vectorSize == 0 || v2.size() == 0)
			return 0;

		int i = -1;
		float product = 0;
		FeatureWeight[] vec1 = v1.sortById();
		while (++i < vec1.length)
			product += (vec1[i].weight * v2.get((int) ((vec1[i].feature - 1) % v2.size())));

		return product;
	}
}
