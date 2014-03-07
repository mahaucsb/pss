package edu.ucsb.cs.partitioning.cosine;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.ucsb.cs.partitioning.Config;
import edu.ucsb.cs.partitioning.PartDriver;
import edu.ucsb.cs.types.FeatureWeightArrayWritable;
import edu.ucsb.cs.types.IdFeatureWeightArrayWritable;
import edu.ucsb.cs.types.IntIntWritable;

/**
 * This Mapper reads in file "i" with a record per line . It emits <key> group
 * folder name Gij <value> for each sequence document coming in based on the
 * locate(document) function.
 * <p>
 * Input record format: KEY:LongWritable, VAULE: FeatureWeightArrayWritable<br>
 * </p>
 */
public class CosineWeightPartMapper extends MapReduceBase
		implements
		Mapper<LongWritable, FeatureWeightArrayWritable, IntIntWritable, IdFeatureWeightArrayWritable> {

	ArrayList<Float> partitionsMaxWeight = new ArrayList<Float>();
	private int rPrefix;
	private float threshold;

	private IntIntWritable outputKey;
	private IdFeatureWeightArrayWritable outputValue = new IdFeatureWeightArrayWritable();

	@Override
	public void configure(JobConf job) { // change it to read from cache +
		threshold = job.getFloat(Config.THRESHOLD_PROPERTY, Config.THRESHOLD_VALUE);
		partitionsMaxWeight.add(0f);
		rPrefix = Integer.parseInt((new Path(job.get("map.input.file"))).getName());
		outputKey = new IntIntWritable(rPrefix);
		try {
			FileSystem hdfs = FileSystem.get(job);
			BufferedReader maxWeightReader = new BufferedReader(new InputStreamReader(
					new DataInputStream(hdfs.open(new Path(job.get(Partitioner.MAX_DIR_PATH))))));
			String line;
			while ((line = maxWeightReader.readLine()) != null) {
				partitionsMaxWeight.add(Float.parseFloat(line.replaceAll("[^.0-9]", "")));
				System.out
						.println("maxw read: " + Float.parseFloat(line.replaceAll("[^.0-9]", ""))); // remove
			}
			maxWeightReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void map(LongWritable id, FeatureWeightArrayWritable record,
			OutputCollector<IntIntWritable, IdFeatureWeightArrayWritable> output, Reporter reporter)
			throws IOException {
		outputKey.y = locate(record);
		outputValue.id = id.get();
		outputValue.vectorSize = record.vectorSize;
		outputValue.vector = record.vector;
		output.collect(outputKey, outputValue);
	}

	public int locate(FeatureWeightArrayWritable record) {
		float thNorm = threshold / record.getNorm1();
		for (int i = rPrefix - 1; i > 0; i--)
			if (thNorm >= partitionsMaxWeight.get(i))
				return i;
		return rPrefix;
	}
}
