package edu.ucsb.cs.sort.signature;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.ucsb.cs.lsh.types.BitSignature;

/**
 * <p>
 * [INPUT] KEY: {@link BitSignature}, VALUE: docId
 * </p>
 * <p>
 * [OUTPUT] KEY: docId, VALUE: {@link BitSignature}
 * </p>
 * 
 * @author maha
 */
public class SigSortReducer extends MapReduceBase implements
		Reducer<BitSignature, LongWritable, LongWritable, BitSignature> {

	public void reduce(BitSignature key, Iterator<LongWritable> inputValues,
			OutputCollector<LongWritable, BitSignature> output, Reporter reporter)
			throws IOException {
		while (inputValues.hasNext()) {
			output.collect(inputValues.next(), key);
		}
	}
}
