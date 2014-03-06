/*
 * 
 */
package edu.ucsb.cs.sort.maxw;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.ucsb.cs.types.FeatureWeightArrayWritable;
import edu.ucsb.cs.types.IdFeatureWeightArrayWritable;

/**
 * This Mapper reads in sequence files with <key> LongWritable as key and
 * <value> FeatureWeightArrayWritable as value. It emits <key> 1-norm of
 * document along with <value> the document as IdFeatureWeightArrayWritable. The
 * documents are comparable based on their 1-norms.
 * <p>
 * Example of an input record is: 3 [19, 115, 204]
 * </p>
 */
public class MaxwSortMapper extends MapReduceBase
		implements
		Mapper<LongWritable, FeatureWeightArrayWritable, FloatWritable, IdFeatureWeightArrayWritable> {

	private FloatWritable outputKey = new FloatWritable();
	private IdFeatureWeightArrayWritable outputValue = new IdFeatureWeightArrayWritable();

	public void map(LongWritable id, FeatureWeightArrayWritable vector,
			OutputCollector<FloatWritable, IdFeatureWeightArrayWritable> output, Reporter reporter)
			throws IOException {

		outputKey.set(vector.getMaxWeight());
		outputValue.id = id.get();
		outputValue.vector = vector.vector;
		outputValue.vectorSize = vector.vectorSize;
		output.collect(outputKey, outputValue);
	}
}