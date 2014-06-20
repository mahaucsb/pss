package edu.ucsb.cs.sort.norm;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

import edu.ucsb.cs.types.IdFeatureWeightArrayWritable;

/**
 * The class partitions keys into ranges and distribute each range to a reducer,
 * such that (range i < j) goes to (reducer m < n).
 * 
 */

@SuppressWarnings("deprecation")
public class NormRangePartitioner implements
		Partitioner<FloatWritable, IdFeatureWeightArrayWritable> {

	private float maxDocNorm;

	public int getPartition(FloatWritable key, IdFeatureWeightArrayWritable value,
			int numReduceTasks) {
		int range = (int) maxDocNorm / numReduceTasks;
		if(range==0)range = 1;
		int reduceNo = (int) key.get() / range;
		if (reduceNo >= numReduceTasks)
			return (numReduceTasks - 1);
		else
			return reduceNo;
	}

	public void configure(JobConf job) {
		maxDocNorm = job.getFloat("max.doc.norm", 5.0f);
	}
}
