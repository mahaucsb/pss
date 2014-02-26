package edu.ucsb.cs.preprocessing.hashing;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;

/**
 * Reads in files of features produced from the previous job into a HashMap
 * sorted decreasingly by popularity to allow pruning of popular terms. It also
 * assignes each term a number as hashing value.
 * 
 * @author Maha Alabduljalil
 */
public abstract class HashMapper extends MapReduceBase implements
		Mapper<Object, Text, Text, NullWritable> {

	/**
	 * Sequential mapping of numbers to popularity-sorted features. Popularity
	 * of a feature is the number of its occurrence in different vectors each
	 * contributing 1.
	 **/
	public HashMap<String, Long> featureHash = new HashMap<String, Long>();
	/** Number of pages processed by this mapper **/
	public long pageCount = 0;

	protected String pagePrefixID;
	protected Text hashedPageKey = new Text();
	protected NullWritable nullValue = NullWritable.get();
	protected int maxFreq;

	/** Used to do the df-cut described by Jimmy Lin */
	private HashMap<String, Integer> featuresPostingLen = new HashMap<String, Integer>();
	private float dfCut;

	@Override
	public void configure(JobConf job) {
		maxFreq = job.getInt(HashPagesDriver.MAX_FEATURE_FREQ_PROPERTY,
				HashPagesDriver.MAX_FEATURE_FREQ_VALUE);
		dfCut = job.getFloat(HashPagesDriver.DF_CUT_PROPERTY, HashPagesDriver.DF_CUT_VALUE);
		pagePrefixID = job.get("mapred.task.partition");
		readFeatures(job);
		dfCutFeatures();
	}

	public void readFeatures(JobConf job) {
		try {
			Path[] localFiles = DistributedCache.getLocalCacheFiles(job);
			if (null != localFiles && localFiles.length > 0) {
				for (Path cachePath : localFiles) {
					addFeatures(cachePath);
				}
			} else
				System.out.println("No files in local cache!");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Reads in the features files produced in a previous job into memory along
	 * with each feature's posting length.
	 * 
	 * @param cachePath : path to the file added to Hadoop distributed cache.
	 * @throws IOException
	 */
	public void addFeatures(Path cachePath) throws IOException {
		BufferedReader wordReader = new BufferedReader(new FileReader(cachePath.toString()));
		try {
			String feature_postingLen;
			while ((feature_postingLen = wordReader.readLine()) != null) {
				StringTokenizer tkz = new StringTokenizer(feature_postingLen);
				String feature = tkz.nextToken();
				int df = Integer.parseInt(tkz.nextToken());
				featuresPostingLen.put(feature, df);
			}
		} finally {
			wordReader.close();
		}
	}

	/**
	 * Prunes away most of popular features by sorting featuresPostingLen map
	 * based on value (ie.posting length allowing duplicates). Then cut off
	 * popular features. Specifically (dfCut * totalNumberFeatures) are removed
	 * where dfCut is the configured percentage. This idea is Jimmy Lin's.
	 */
	public void dfCutFeatures() {

		List<Map.Entry<String, Integer>> entries = new ArrayList<Map.Entry<String, Integer>>(
				featuresPostingLen.entrySet());
		Collections.sort(entries, new Comparator<Entry<String, Integer>>() {
			public int compare(Entry<String, Integer> e1, Entry<String, Integer> e2) {
				return e2.getValue().compareTo(e1.getValue());
			}
		});

		Map<String, Integer> sortedFeaturesByPopularity = new LinkedHashMap<String, Integer>();
		for (Entry<String, Integer> entry : entries) {
			sortedFeaturesByPopularity.put(entry.getKey(), entry.getValue());
		}

		int numSkipFeatures = (int) (Math.ceil(sortedFeaturesByPopularity.size() * dfCut));
		Iterator<String> itr = sortedFeaturesByPopularity.keySet().iterator();
		while (numSkipFeatures > 0) {
			numSkipFeatures--;
			itr.next();
		}
		long featureCount = 0;
		while (itr.hasNext()) {
			featureHash.put(itr.next(), (++featureCount));
		}
	}
}