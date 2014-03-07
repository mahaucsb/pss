package edu.ucsb.cs.preprocessing.hashing;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * This map class reads in page by page from the text input directory and output
 * it in text with sorted MD5 hashed features instead of the original words,
 * along with each feature normalized weight. Notice, the weights plotted are
 * only for those with low frequencies. High frequent words are eliminated. <br>
 * <br>
 * 
 * <b>Example:</b><br>
 * Input: www.amazon.com hadoop filesystem hadoop <br>
 * Output: 1 0.01 2 0.01 3 0.01 <br>
 * where 1=filesystem, 2=hadoop, 3=www.amazon.com and 1-norm isn't one here
 * because we dropped some those features with posting length=1, hence the use
 * of the Pruned/Index-hashFreq.
 * 
 * @author Maha Alabduljalil
 */
public class Md5FeatureBinaryWeightMapper extends Md5FeatureWeightMapper {

	// @Override
	// public void configure(JobConf job) {
	// System.out.println("Im hereeeeee 1"); // remove
	// super.configure(job);
	// }

	@Override
	public void map(Object unused, Text page, OutputCollector<Text, NullWritable> output,
			Reporter reporter) throws IOException {

		long feature;
		pageCount++;
		StringTokenizer words = new StringTokenizer(page.toString(), " \t\n\r\f");
		StringBuilder hashPage = new StringBuilder(pagePrefixID + pageCount + " ");
		IndexhashFreq.clear();

		double sqrtSum = fillHashFreq(words);

		// Sorted based on feature id
		Iterator<Long> features = (new TreeSet(IndexhashFreq.keySet())).iterator();
		while (features.hasNext()) {
			feature = features.next();
			if (IndexhashFreq.get(feature) <= maxFreq) // remove high frequency
				hashPage.append(feature + " " + (float) (IndexhashFreq.get(feature) / sqrtSum)
						+ " ");
		}

		this.hashedPageKey.set(hashPage.toString());
		output.collect(hashedPageKey, nullValue);
	}

	@Override
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
				feature = getMD5(word, 8);
				featureHash.put(word, feature);
				IndexhashFreq.put(feature, (long) 1);
			}
		}
		return getSqrtSquaredSum();
	}

}