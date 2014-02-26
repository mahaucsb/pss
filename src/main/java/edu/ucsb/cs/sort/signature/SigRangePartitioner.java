package edu.ucsb.cs.sort.signature;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

import edu.ucsb.cs.lsh.types.BitSignature;

/**
 * The class partitions keys into ranges and distribute each range to a reducer,
 * such that range i < range j goes to reducer m < reducer n.
 * 
 * @param <FloatWritable> maxw
 * @param <IdFeatureWeightArrayWritable> document
 */

@SuppressWarnings("deprecation")
public class SigRangePartitioner implements Partitioner<BitSignature, LongWritable> {

	private float maxSignature;

	public int getPartition(BitSignature key, LongWritable value, int numReduceTasks) {
		float range = maxSignature / numReduceTasks;
		int reduceNo = (int) (key.convertToInt() / range);
		if (reduceNo >= numReduceTasks)
			return (numReduceTasks - 1);
		else
			return reduceNo;
	}

	public void configure(JobConf job) {
		maxSignature = job.getFloat("max.sig.num", 1983);
	}
}
