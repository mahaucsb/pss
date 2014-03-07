package edu.ucsb.cs.preprocessing.hashing;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.ucsb.cs.preprocessing.Config;

/**
 * Reads in a pair of feature and a list of document IDs it appeared in. Then
 * output features which have appeared in at least two vector along with its
 * posting length.:<br>
 * 
 * <pre>
 * READ: KEY:&ltfeature&gt, VALUE: list of vector IDs. 
 * EMIT: KEY:&ltfeature&gt, VALUE: posting length
 * </pre>
 * 
 * where features are those which appeared at least twice in the corpse from
 * different vectors. Meaning we do not accept a feature appearing many times in
 * a single vector.<br>
 * <br>
 * 
 * 
 * @author Maha Alabduljalil
 * 
 */
public class InvertedIndexReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, IntWritable> {

	private int minPostingLength = 1;
	private IntWritable value = new IntWritable();

	@Override
	public void configure(JobConf job) {
		super.configure(job);
		if (job.getBoolean(Config.LONELY_FEATURES_PROPERTY,
				Config.LONELY_FEATURES_VALUE))
			minPostingLength = 0;
	}

	public void reduce(Text word, Iterator<Text> docs, OutputCollector<Text, IntWritable> output,
			Reporter reporter) throws IOException {

		HashSet<Text> uniqueDocs = new HashSet<Text>();
		while (docs.hasNext())
			uniqueDocs.add(docs.next());
		if (uniqueDocs.size() > minPostingLength) {
			value.set(uniqueDocs.size());
			output.collect(word, value);
		}
	}
}
