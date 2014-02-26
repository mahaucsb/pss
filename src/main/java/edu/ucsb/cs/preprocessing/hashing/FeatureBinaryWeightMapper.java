package edu.ucsb.cs.preprocessing.hashing;

import java.util.StringTokenizer;

/**
 * This map class reads in the file of features produced from the previous job
 * into a HashMap then it reads in page by page from the text input directory
 * and output it with hashed features values instead of the original words
 * sorted along with each feature normalized weight. Notice, the weights are
 * assumed to be from binary frequencies.<br>
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
public class FeatureBinaryWeightMapper extends FeatureWeightMapper {

	@Override
	public double fillHashFreq(StringTokenizer words) {
		String word;
		while (words.hasMoreTokens()) {
			word = words.nextToken();
			if (featureHash.containsKey(word))
				IndexhashFreq.put(featureHash.get(word), (long) 1);
			else
				PrunedhashFreq.put(word, (long) 1);
		}
		return getSqrtSquaredSum();
	}
}
