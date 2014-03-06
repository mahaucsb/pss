package edu.ucsb.cs.partitioning.cosine;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.ucsb.cs.types.FeatureWeightArrayWritable;
import edu.ucsb.cs.types.IdFeatureWeightArrayWritable;
import edu.ucsb.cs.types.IntIntWritable;

/**
 * <p>
 * [INPUT] KEY: G i j , VALUE: document
 * </p>
 * <p>
 * [OUTPUT] KEY: id VALUE: document-id
 * </p>
 * 
 * @author maha
 */
public class CosinePartReducer extends MapReduceBase
		implements
		Reducer<IntIntWritable, IdFeatureWeightArrayWritable, LongWritable, FeatureWeightArrayWritable> {

	private LongWritable outputKey = new LongWritable();
	private FeatureWeightArrayWritable outputValue = new FeatureWeightArrayWritable();

	public void reduce(IntIntWritable key, Iterator<IdFeatureWeightArrayWritable> inputValues,
			OutputCollector<LongWritable, FeatureWeightArrayWritable> output, Reporter reporter)
			throws IOException {
		while (inputValues.hasNext()) {
			IdFeatureWeightArrayWritable element = inputValues.next();
			outputKey.set(element.id);
			outputValue.vectorSize = element.vectorSize;
			outputValue.vector = element.vector;
			output.collect(outputKey, outputValue);
		}
	}
}
