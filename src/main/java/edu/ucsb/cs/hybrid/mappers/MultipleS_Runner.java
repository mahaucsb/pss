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
 * @Since Jul 27, 2012
 */

package edu.ucsb.cs.hybrid.mappers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.ucsb.cs.hybrid.Config;
import edu.ucsb.cs.hybrid.io.Reader;
import edu.ucsb.cs.types.DocDocWritable;
import edu.ucsb.cs.types.PostingDocWeight;

/*
 * Errors corrected:
 * splitsSize = (int) Math.ceil(S_size / nSplits); // wrong!
 * Limitation: doesn't work when hadoop.splittable = true; to fix, un-comment the lines in configure.
 * Not sure about that though, mainly not sure if logical splits can be read via "map.input.file" thing.
 */
public class MultipleS_Runner extends SingleS_Runner {

	protected MultipleS_Mapper mapper;
	protected int nSplits, splitsSize, splitNum = 0;

	@Override
	public void configure(JobConf job) {
		super.configure(job);
		nSplits = conf.getInt(Config.NUMBER_SPLITS_PROPERTY, Config.NUMBER_SPLITS_VALUE);
		nSplits = (nSplits <=0)? 1: nSplits;
		// long S_size = Collector.countFileVectors(FileSystem.get(job),
		// new Path(job.get("map.input.file")), conf); //accepts different S.
		long S_size = job.getLong(Config.MAP_S_PROPERTY, Config.MAP_S_VALUE);
		splitsSize = (int) Math.ceil(S_size / (float) nSplits);
		mapper.IdMaps = new ArrayList<long[]>(nSplits);
	}

	@Override
	public void instantMapper(JobConf job) {
		ClassLoader myClassLoader = ClassLoader.getSystemClassLoader();
		try {
			Class mapperClass = myClassLoader.loadClass(job.getMapperClass().getName());
			mapper = (MultipleS_Mapper) mapperClass.newInstance();
			mapper.configure(job);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Passes the built inverted indexes to the map task.
	 */
	@Override
	protected void initMapper(Object splits, boolean logV, boolean idComparisonV) {
		mapper.initialize((ArrayList<HashMap<Long, PostingDocWeight[]>>) splits, logV,
				idComparisonV, splitsSize);
	}

	/**
	 * Builds "nSplits" inverted indexes specified by the configuration hybrid.number.splits.
	 */
	@Override
	public Object buildInvertedIndex(boolean log) throws IOException {
		ArrayList<HashMap<Long, PostingDocWeight[]>> invertedIndexes = new ArrayList<HashMap<Long, PostingDocWeight[]>>();
		for (splitNum = 0; splitNum < nSplits; splitNum++) {
			invertedIndexes.add(splitNum,
					(HashMap<Long, PostingDocWeight[]>) super.buildInvertedIndex(log));
		}
		return invertedIndexes;
	}

	@Override
	protected void closeMapper() throws IOException {
		mapper.close();
	}

	@Override
	public long[] initIdMap(int s) {
		long[] idMap = new long[s];
		mapper.IdMaps.add(splitNum, idMap);
		return getIdMap();
	}

	@Override
	public long[] getIdMap() {
		return mapper.IdMaps.get(splitNum);
	}

	@Override
	public void comparePartition(Reader reader,
			OutputCollector<DocDocWritable, FloatWritable> output, Reporter reporter)
			throws IOException {
		mapper.compareWith(reader, output, reporter);
	}

	@Override
	public boolean splitLimitReached(int nVectors) {
		if (splitsSize == 0)
			throw new UnsupportedOperationException(
					"Number of S splits is too big for this input !!!");
		if (nVectors != 0 && (nVectors % splitsSize == 0))
			return true;
		else
			return false;
	}
}