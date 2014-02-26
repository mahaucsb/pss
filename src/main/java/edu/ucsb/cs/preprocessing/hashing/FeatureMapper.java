package edu.ucsb.cs.preprocessing.hashing;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * This map class reads in the file of features produced from the previous job
 * into a HashMap then it reads in page by page from the text input directory
 * and output it with hashed features values instead of the original words along
 * with each feature normalized weight.<br>
 * <br>
 * 
 * <b>Example:</b><br>
 * Input: www.amazon.com hadoop filesystem <br>
 * Output: 1 2 3 <br>
 * where 1=filesystem, 2=hadoop, 3=www.amazon.com
 * 
 * @author Maha Alabduljalil
 */
public class FeatureMapper extends HashMapper {

	private HashSet<Long> wordhashes = new HashSet<Long>();

	public void map(Object unused, Text page, OutputCollector<Text, NullWritable> output,
			Reporter reporter) throws IOException {

		pageCount++;
		wordhashes.clear();
		StringTokenizer words = new StringTokenizer(page.toString(), " ");
		StringBuilder hashPage = new StringBuilder(pagePrefixID + pageCount + " ");

		while (words.hasMoreTokens()) {
			String word = words.nextToken();
			if (featureHash.containsKey(word))
				wordhashes.add(featureHash.get(word));
		}
		Iterator<Long> itr = (new TreeSet(wordhashes)).iterator();
		while (itr.hasNext())
			hashPage.append(itr.next() + " ");

		this.hashedPageKey.set(hashPage.toString());
		output.collect(hashedPageKey, nullValue);
	}
}
