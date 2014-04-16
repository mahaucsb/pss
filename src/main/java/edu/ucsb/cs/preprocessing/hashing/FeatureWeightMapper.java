package edu.ucsb.cs.preprocessing.hashing;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * Reads in the file of features produced from the previous job into a HashMap
 * then it reads in page by page from the text input directory and output it
 * with hashed features values instead of the original words sorted along with
 * each feature normalized weight. Notice, the weights are wrt the original
 * page features, but eliminates those pruned features such as those appearing
 * once in the corpse and cutting off some under dfCut. <br>
 * <br>
 * 
 * <b>Example:</b><br>
 * Input: www.amazon.com hadoop filesystem hadoop <br>
 * Output: 001 1 0.01 2 0.01 3 0.002 <br>
 * where 001=ID 1=filesystem, 2=hadoop, 3=www.amazon.com and 1-norm isn't one
 * here because we dropped some those features with posting length=1. <br>
 * 
 * @author Maha Alabduljalil
 */
public class FeatureWeightMapper extends HashMapper {

	/**
	 * Indexed features from pages that are to be read into memory for
	 * similarity computing.
	 */
	protected HashMap<Long, Long> IndexhashFreq = new HashMap<Long, Long>();
	/**
	 * Pruned features (occurred once) of this page to be used for normalizing
	 * the page, but excluded from similarity comparison.
	 */
	protected HashMap<String, Long> PrunedhashFreq = new HashMap<String, Long>();

	public void map(Object unused, Text page, OutputCollector<Text, NullWritable> output,
			Reporter reporter) throws IOException {

		long feature;
		pageCount++;
		StringTokenizer words = new StringTokenizer(page.toString(), " \t\n\r\f:");
		StringBuilder hashPage = new StringBuilder(pagePrefixID + pageCount + " ");
		IndexhashFreq.clear();
		PrunedhashFreq.clear();

		if(words.hasMoreTokens()){ 
			writeIdsMapping(pagePrefixID + pageCount+" :: "+words.nextToken());
		}
		double sqrtSum = fillHashFreq(words);

		Iterator<Long> features = (new TreeSet(IndexhashFreq.keySet())).iterator();
		while (features.hasNext()) {
			feature = features.next();
			if (IndexhashFreq.get(feature) <= maxFreq) // remove high frequent features
				hashPage.append(feature + " " + (float) (IndexhashFreq.get(feature) / sqrtSum)
						+ " ");
		}

		this.hashedPageKey.set(hashPage.toString());
		output.collect(hashedPageKey, nullValue);
	}

	
	public double fillHashFreq(StringTokenizer words) {
		long feature;
		String word;
		while (words.hasMoreTokens()) {
			word = words.nextToken();
			if (featureHash.containsKey(word)) {
				feature = featureHash.get(word);
				if (IndexhashFreq.containsKey(feature))
					IndexhashFreq.put(feature, IndexhashFreq.get(feature) + 1);
				else
					IndexhashFreq.put(feature, (long) 1);
			} else {
				if (PrunedhashFreq.containsKey(word))
					PrunedhashFreq.put(word, PrunedhashFreq.get(word) + 1);
				else
					PrunedhashFreq.put(word, (long) 1);
			}
		}
		return getSqrtSquaredSum();
	}

	public double getSqrtSquaredSum() {
		double sum = 0;
		Iterator<Long> iItr = IndexhashFreq.keySet().iterator();
		while (iItr.hasNext()) {
			long freq = IndexhashFreq.get(iItr.next());
			sum += (freq * freq);
		}
		/* Optional part: include lonely features in weighting system */
		Iterator<String> pItr = PrunedhashFreq.keySet().iterator();
		while (pItr.hasNext()) {
			long freq = PrunedhashFreq.get(pItr.next());
			sum += (freq * freq);
		}
		return Math.sqrt(sum);
	}
}
