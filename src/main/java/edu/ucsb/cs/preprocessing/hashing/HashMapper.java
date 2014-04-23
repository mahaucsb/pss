package edu.ucsb.cs.preprocessing.hashing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;

import edu.ucsb.cs.preprocessing.Config;

/**
 * Reads in files of [id:features] produced from the previous job into a HashMap
 * sorted decreasingly by popularity to allow pruning of popular terms. It also
 * assigns each term a number as hashing value. map function needs to be implemented. 
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

	/** This is used to convert back the hashed ids to actual in Hybrid */
	protected BufferedWriter br=null;

	@Override
	public void configure(JobConf job) {
		maxFreq = job.getInt(Config.MAX_FEATURE_FREQ_PROPERTY,
				Config.MAX_FEATURE_FREQ_VALUE);
		dfCut = job.getFloat(Config.DF_CUT_PROPERTY, Config.DF_CUT_VALUE);
		pagePrefixID = job.get("mapred.task.partition");
		readFeaturesIntoMemory(job);
		dfCutFeatures();
		openIdsMapping(HashPagesDriver.IDS_FILE1+"/"+pagePrefixID);
	}

	public void readFeaturesIntoMemory(JobConf job) {
		try {
			Path[] localFiles = DistributedCache.getLocalCacheFiles(job); //cache files recall
			if (null != localFiles && localFiles.length > 0) {
				for (Path cachePath : localFiles) {
					addFeaturesIntoIndex(cachePath);
				}
			} else
				throw new UnsupportedEncodingException("ERROR: No files in local cache!");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Reads in the features files produced in a previous job into memory. The
	 * files format is feature along with each feature's posting length per line.
	 * 
	 * @param cachePath : path to the features file added to Hadoop distributed cache.
	 * @throws IOException
	 */

	public void addFeaturesIntoIndex(Path cachePath) throws IOException {
		BufferedReader featuresReader = new BufferedReader(new FileReader(cachePath.toString())); //check Error if local
		String feature_postingLen;
		try {
			while ((feature_postingLen = featuresReader.readLine()) != null) {
				StringTokenizer tkz = new StringTokenizer(feature_postingLen);
				String feature = tkz.nextToken();
				int df = Integer.parseInt(tkz.nextToken());
				featuresPostingLen.put(feature, df);
			}
		}catch(NoSuchElementException e){ 
			throw new UnsupportedOperationException("ERROR: features/ directory is not in HDFS");
		} finally {
			featuresReader.close();
		}
	}

	/**
	 * Prunes away fraction of the popular features after sorting featuresPostingLen 
	 * based on value (ie.posting length allowing duplicates). Then cut off
	 * popular features. Specifically (dfCut * totalNumberFeatures) are removed
	 * where dfCut is a configured percentage. [Elsayed HLT'08].
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

	/**
	 * Opens a text file to hold the mapping serial number "::" actual IDs per line to be used
	 * for converting back to original IDs
	 * @param job
	 * @param outputfile
	 */
	public void openIdsMapping(String outputfile){
		try{
			Path pt=new Path(outputfile);
			FileSystem fs = FileSystem.get(new Configuration()); //or job
			br=new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
		}catch (IOException e){
			System.err.println("Error: " + e.getMessage());
		}
	}

	public void close(){
		closeIdsMapping();
	}

	/**
	 * Closes the text file holding the mapping serial number "::" actual IDs per line to be used
	 * for converting back to original IDs
	 * @param job
	 * @param outputfile
	 */
	public void closeIdsMapping(){
		try{
			br.close();
		}catch (IOException e){
			System.err.println("Error: " + e.getMessage());
		}
	}


	/**
	 * Writes to the text file one mapping. Ie. serial number "::" actual IDs per line to be used
	 * for converting back to original IDs
	 * @param job
	 * @param outputfile
	 */
	public void writeIdsMapping(String mapping){
		try{
			br.write(mapping+"\n");
		}catch (IOException e){
			System.err.println("Error: " + e.getMessage());
		}
	}


}
