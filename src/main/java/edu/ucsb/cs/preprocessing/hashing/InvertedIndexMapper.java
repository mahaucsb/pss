package edu.ucsb.cs.preprocessing.hashing;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * Reads text files and output their words paired with generated document ID.
 * These generated document IDs are unique among all maps and are alter hashed
 * using MD5 to make the distribution balanced.
 * 
 * @author Maha Alabduljalil
 */
public class InvertedIndexMapper extends MapReduceBase implements Mapper<Object, Text, Text, Text> {

	private Text word = new Text();
	private Text postingId = new Text();
	protected String pagePrefixID;
	private long pageCount = 0;

	@Override
	public void configure(JobConf job) {
		pagePrefixID = job.get("mapred.task.partition");
	}

	public void map(Object unused, Text page, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		pageCount++;
		StringTokenizer words = new StringTokenizer(page.toString(), " :\t\r\n");
		if(words.hasMoreTokens())
			words.nextToken();//ignore title
		while (words.hasMoreTokens()) {
			word.set(words.nextToken());
			postingId.set(pagePrefixID + pageCount);
			output.collect(word, postingId);
		}
	}
}
