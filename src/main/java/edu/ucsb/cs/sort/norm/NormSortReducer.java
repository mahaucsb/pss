package edu.ucsb.cs.sort.norm;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.ucsb.cs.types.FeatureWeightArrayWritable;
import edu.ucsb.cs.types.IdFeatureWeightArrayWritable;

/**
 * <p>
 * [INPUT] KEY: p-norm, VALUE: document
 * </p>
 * <p>
 * [OUTPUT] KEY: document, VALUE: null
 * </p>
 * 
 * @author maha
 */
public class NormSortReducer extends MapReduceBase
		implements
		Reducer<FloatWritable, IdFeatureWeightArrayWritable, LongWritable, FeatureWeightArrayWritable> {

	private LongWritable outputKey = new LongWritable();
	private FeatureWeightArrayWritable outputValue = new FeatureWeightArrayWritable();

	public void reduce(FloatWritable key, Iterator<IdFeatureWeightArrayWritable> inputValues,
			OutputCollector<LongWritable, FeatureWeightArrayWritable> output, Reporter reporter)
			throws IOException {
		while (inputValues.hasNext()) {
			IdFeatureWeightArrayWritable record = inputValues.next();
			outputKey.set(record.id);
			outputValue.vectorSize = record.vectorSize;
			outputValue.vector = record.vector;
			output.collect(outputKey, outputValue);
		}
	}
}
