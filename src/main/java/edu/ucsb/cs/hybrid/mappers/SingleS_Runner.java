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
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapRunner;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import edu.ucsb.cs.hybrid.Config;
import edu.ucsb.cs.hybrid.io.OneMapReader;
import edu.ucsb.cs.hybrid.io.Reader;
import edu.ucsb.cs.types.DocDocWritable;
import edu.ucsb.cs.types.FeatureWeightArrayWritable;
import edu.ucsb.cs.types.PostingDocWeight;

public class SingleS_Runner extends
MapRunner<LongWritable, FeatureWeightArrayWritable, DocDocWritable, FloatWritable> {

	protected JobConf conf;
	protected SingleS_Mapper mapper;
	/** Number of vectors assigned to each map task */
	public int S_size = 0; //counted here while reading
	protected long totalTerms = 0;
	boolean googleDynSkip; // late
	ArrayList<Float> dynamicSmaxw = null; // late

	RecordReader<LongWritable, FeatureWeightArrayWritable> input;
	private static final Logger LOG = Logger.getLogger(SingleS_Runner.class);

	/**
	 * Responsible for instantiating and configuring a map class according to
	 * user choice of similarity method.
	 */
	@Override
	public void configure(JobConf job) {
		conf = job;
		instantMapper(job);
	}

	public void instantMapper(JobConf job) {
		ClassLoader myClassLoader = ClassLoader.getSystemClassLoader();
		try {
			Class mapperClass = myClassLoader.loadClass(job.getMapperClass().getName());
			mapper = (SingleS_Mapper) mapperClass.newInstance();
			mapper.configure(job);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Called automatically by Hadoop framework to control the flow of functions
	 * calls in the map tasks. Here, it will read the whole split of the s
	 * assigned vectors into memory S. Due to unknown size of the split, we
	 * first use dynamic data structures then convert it to static ones before
	 * APSS.
	 */
	@Override
	public void run(RecordReader<LongWritable, FeatureWeightArrayWritable> input,
			OutputCollector<DocDocWritable, FloatWritable> output, Reporter reporter)
					throws IOException {
		this.input = input;
		boolean log = conf.getBoolean(Config.LOG_PROPERTY, Config.LOG_VALUE);
		boolean origIdComp, idComparison = !conf.getBoolean(Config.CIRCULAR_PROPERTY,
				Config.CIRCULAR_VALUE);
		googleDynSkip = conf.getBoolean(Config.BAYADRO_SKIP_PROPERTY, Config.BAYADRO_SKIP_VALUE); // late
		HashMap<Long, ArrayList<PostingDocWeight>> dynSIndex = null;

		//
		// Phase1: Build Index
		//
		long startTime = System.currentTimeMillis();
		Object II = buildInvertedIndex(log);
		if (II == null)
			return;
		initMapper(II, log, idComparison);
		System.out.println("LOG: Inverted index building time in millisec:" + (System.currentTimeMillis() - startTime));

		//
		// Phase2: Similarity Computations
		//
		Reader reader = getReader(conf);
		if (log) {
			System.out.println("LOG: Total files to compare with: " + reader.nFiles);//check
		}
		startTime = System.nanoTime();

		for (int currentFile = 0; currentFile < reader.nFiles; currentFile++) {
			if (!reader.setReader(currentFile))
				continue;
			else {
				if (reader.readMyPartition) {
					origIdComp = idComparison;
					idComparison = true;
					comparePartition(reader, output, reporter);
					idComparison = origIdComp;
				} else {
					comparePartition(reader, output, reporter);
				}
			}
		}
		//		System.out.println("Similarity comparison time in millisec:"
		//				+ (System.nanoTime() - startTime) / 1000000.0);
		closeMapper();
	}

	public void comparePartition(Reader reader,
			OutputCollector<DocDocWritable, FloatWritable> output, Reporter reporter)
					throws IOException {
		mapper.compareWith(reader, output, reporter);
	}

	protected void closeMapper() throws IOException {
		mapper.close();
	}

	@Override
	protected Mapper getMapper() {
		return mapper;
	}

	protected void initMapper(Object split, boolean logV, boolean idComparisonV) {
		mapper.initialize((HashMap<Long, PostingDocWeight[]>) split, logV, idComparisonV, S_size);
	}

	public boolean splitLimitReached(int nVectors) {
		return false;
	}

	public Object buildInvertedIndex(boolean log) throws IOException {
		HashMap<Long, ArrayList<PostingDocWeight>> dynSIndex = new HashMap<Long, ArrayList<PostingDocWeight>>();
		ArrayList<Long> dynamicIdMap = null;
		LongWritable doc = new LongWritable();
		FeatureWeightArrayWritable vector = new FeatureWeightArrayWritable();
		totalTerms = 0;
		int nVectors = 0;
		dynamicIdMap = new ArrayList<Long>();

		if (googleDynSkip) 
			dynamicSmaxw = new ArrayList<Float>();

		long startTime = System.currentTimeMillis();
		while (!splitLimitReached(nVectors) && input.next(doc, vector)) {
			dynamicIdMap.add(doc.get()); // index 0,1,2...
			if (googleDynSkip)
				dynamicSmaxw.add(vector.getMaxWeight());
			int numTerms = vector.getSize();
			totalTerms += numTerms;
			for (int i = 0; i < numTerms; i++) {
				ArrayList<PostingDocWeight> array = getPosting(dynSIndex, vector.getFeature(i));
				PostingDocWeight item = new PostingDocWeight(nVectors, vector.getWeight(i));
				array.add(item);
			}
			nVectors++;
		}
		S_size += nVectors;
		if (log) {
			long distinctTerms = dynSIndex.size();
			System.out.println("LOG: Build inverted index time in millisec:"
					+ ((System.currentTimeMillis() - startTime))
					+ "\nLOG: Number of distict features:" + distinctTerms
					+ "\nLOG: Reduction in features storage:"
					+ ((totalTerms - distinctTerms) / (float) totalTerms * 100)
					+ "\nAvg posting length per feature:" + totalTerms / (float) distinctTerms);
		}
		if(S_size ==0)
			return null;
		else
			return convertDynamicStatic(dynSIndex, dynamicIdMap);
	}

	public ArrayList<PostingDocWeight> getPosting(
			HashMap<Long, ArrayList<PostingDocWeight>> dynSIndex, long t) {
		if (!dynSIndex.containsKey(t)) {
			dynSIndex.put(t, new ArrayList<PostingDocWeight>());
		}
		return dynSIndex.get(t);
	}

	public static Reader getReader(JobConf conf) throws IOException {
		boolean oneMap = conf.getBoolean(Config.SINGLE_MAP_PROPERTY, Config.SINGLE_MAP_VALUE);
		boolean splittable = conf.getBoolean(Config.SPLITABLE_PROPERTY, Config.SPLITABLE_VALUE);

		if (!oneMap || splittable)
			// if (conf.getBoolean(Config.BALANCED_READER_PROPERTY,
			// Config.BALANCED_READER_VALUE))
			// return new BalancedReader(conf, new
			// Path(conf.get("map.input.file")), conf.getInt(
			// Config.COMP_BLOCK_PROPERTY, Config.COMP_BLOCK_VALUE));
			// else
			return new Reader(conf, new Path(conf.get("map.input.file")), conf.getInt(
					Config.COMP_BLOCK_PROPERTY, Config.COMP_BLOCK_VALUE));
		else
			return new OneMapReader(conf, new Path(conf.get("map.input.file")), conf.getInt(
					Config.COMP_BLOCK_PROPERTY, Config.COMP_BLOCK_VALUE));
	}

	public HashMap<Long, PostingDocWeight[]> convertDynamicStatic(
			HashMap<Long, ArrayList<PostingDocWeight>> dynSIndex, ArrayList<Long> dynamicIdMap) {
		//PART1: convert the dynamic list inverted index
		int i = 0, s;
		PostingDocWeight item;
		long term;
		ArrayList<PostingDocWeight> array;
		HashMap<Long, PostingDocWeight[]> SplitIndex = new HashMap<Long, PostingDocWeight[]>();
		Iterator<Long> terms = dynSIndex.keySet().iterator();

		long[] terms_new = new long[dynSIndex.keySet().size()];
		for (i = 0; i < terms_new.length; i++)
			terms_new[i] = terms.next();

		//PART2: convert the dynamic list of document IDs
		System.err.println("errorr: converting idMap ");//remove
		convertIdMap(dynamicIdMap);
		if (googleDynSkip) {
			convertSmaxw(dynamicSmaxw); 
		}
		System.gc();

		for (int h = 0; h < terms_new.length; h++) {
			term = terms_new[h];
			array = dynSIndex.remove(term);
			s = array.size();

			PostingDocWeight[] toStaticArrayII = new PostingDocWeight[s];
			for (i = 0; i < s; i++) {
				item = array.get(0);
				toStaticArrayII[i] = new PostingDocWeight(item.doc, item.weight);
				array.remove(0);
			}
			Arrays.sort(toStaticArrayII, new indexComparable());
			array = null;
			SplitIndex.put(term, toStaticArrayII);
		}
		dynSIndex.clear();
		dynSIndex = null;
		System.gc();
		return SplitIndex;
	}

	/**
	 * Converts the dynamic lists into static arrays to reduce memory occupied
	 * @param dynamicIdMap
	 */
	public void convertIdMap(ArrayList<Long> dynamicIdMap) {
		int s = dynamicIdMap.size();
		long[] idMap = initIdMap(s);
		System.err.println("errorr:s= "+s);//remove
		for (int i = 0; i < s; i++) {
			idMap[i] = dynamicIdMap.get(0);
			System.err.println("errorr: adding "+idMap[i]);//remove
			dynamicIdMap.remove(0);
		}
		dynamicIdMap.clear();
		dynamicIdMap = null;
	}

	public void convertSmaxw(ArrayList<Float> dynamicSmaxw) { // late
		int s = dynamicSmaxw.size();
		float[] maxwS = initSmaxw(s);
		for (int i = 0; i < s; i++) {
			maxwS[i] = dynamicSmaxw.get(0);
			dynamicSmaxw.remove(0);
		}
		dynamicSmaxw.clear();
		dynamicSmaxw = null;
	}

	public float[] initSmaxw(int s) {// late
		mapper.maxwS = new float[s];
		return getSmaxw();
	}

	public float[] getSmaxw() {// late
		return mapper.maxwS;
	}

	public long[] initIdMap(int s) {
		mapper.IdMap = new long[s];
		return getIdMap();
	}

	public long[] getIdMap() {
		return mapper.IdMap;
	}

	class indexComparable implements Comparator<PostingDocWeight> {
		public int compare(PostingDocWeight o1, PostingDocWeight o2) {
			if (getIdMap()[o1.doc] < getIdMap()[o2.doc])
				return -1;
			else
				return 1;
		}
	}

	public void printSInvertedIndex(HashMap<Long, ArrayList<PostingDocWeight>> TermsTree) {
		System.out.println("========= In-memory II =========");
		ArrayList<PostingDocWeight> array;
		long term;
		int postLen, h;
		Iterator<Long> terms = TermsTree.keySet().iterator();
		while (terms.hasNext()) {
			term = terms.next();
			System.out.println(term + "-->");
			array = TermsTree.get(term);
			postLen = array.size();
			for (h = 0; h < postLen; h++) {
				System.out.println("(" + array.get(h).doc + "," + array.get(h).weight + ")");
			}
		}
	}
}