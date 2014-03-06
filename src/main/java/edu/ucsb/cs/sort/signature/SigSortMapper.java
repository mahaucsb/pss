/*
 * 
 */
package edu.ucsb.cs.sort.signature;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.ucsb.cs.lsh.types.BitSignature;
import edu.ucsb.cs.types.IdFeatureWeightArrayWritable;

/**
 * This Mapper reads in sequence files with <key> LongWritable as key and
 * <value> BitSignature as value. It emits <key> BitSignature and <value>
 * LongWritable. The documents are comparable based on their signatures.
 */
public class SigSortMapper extends MapReduceBase implements
		Mapper<LongWritable, BitSignature, BitSignature, LongWritable> {

	private FloatWritable outputKey = new FloatWritable();
	private IdFeatureWeightArrayWritable outputValue = new IdFeatureWeightArrayWritable();

	public void map(LongWritable id, BitSignature sig,
			OutputCollector<BitSignature, LongWritable> output, Reporter reporter)
			throws IOException {
		output.collect(sig, id);
	}
}