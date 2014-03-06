package edu.ucsb.cs.sort.length;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

import edu.ucsb.cs.types.IdFeatureWeightArrayWritable;

/**
 * The class partitions keys into ranges and distribute each range to a reducer,
 * such that range i < range j goes to reducer m < reducer n.
 * 
 * @param <FloatWritable>
 *            maxw
 * @param <IdFeatureWeightArrayWritable>
 *            document
 */

@SuppressWarnings("deprecation")
public class LengthRangePartitioner implements
		Partitioner<FloatWritable, IdFeatureWeightArrayWritable> {

	private float maxDocLength;

	public int getPartition(FloatWritable key, IdFeatureWeightArrayWritable value,
			int numReduceTasks) {
		float range = maxDocLength / numReduceTasks;
		int reduceNo = (int) (key.get() / range);
		if (reduceNo >= numReduceTasks)
			return (numReduceTasks - 1);
		else
			return reduceNo;
	}

	public void configure(JobConf job) {
		maxDocLength = job.getFloat("max.doc.length", 800);
	}
}
