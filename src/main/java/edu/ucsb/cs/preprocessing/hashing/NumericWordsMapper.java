package edu.ucsb.cs.preprocessing.hashing;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * This map class reads in the file of features mapping to numeric values
 * produced from the previous job into a HashMap then it reads in page by page
 * from the text input directory and output it with hashed bag of words instead
 * of the original words. It also assigned a document id to each document.<br>
 * <br>
 * 
 * @author Maha Alabduljalil
 */
public class NumericWordsMapper extends HashMapper {

	public void map(Object unused, Text page, OutputCollector<Text, NullWritable> output,
			Reporter reporter) throws IOException {

		String word;

		pageCount++;
		StringTokenizer words = new StringTokenizer(page.toString(), " \t\n\r\f:");
		StringBuilder hashPage = new StringBuilder(pagePrefixID + pageCount + " ");
		
		if(words.hasMoreTokens()){ 
			writeIdsMapping(pagePrefixID + pageCount+" :: "+words.nextToken());
		}
		while (words.hasMoreTokens()) {
			word = words.nextToken();
			if (featureHash.get(word) != null)
				hashPage.append(featureHash.get(word) + " ");
		}
		this.hashedPageKey.set(hashPage.toString());
		output.collect(hashedPageKey, nullValue);
	}
}
