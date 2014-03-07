package edu.ucsb.cs.partitioning.cosine;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.StringTokenizer;

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
public class CosineAllPartMapper extends MapReduceBase
		implements
		Mapper<LongWritable, FeatureWeightArrayWritable, IntIntWritable, IdFeatureWeightArrayWritable> {

	ArrayList<NormWeightSize> partitionsMaxes = new ArrayList<NormWeightSize>();
	private int rPrefix;
	private float threshold;

	private IntIntWritable outputKey;
	private IdFeatureWeightArrayWritable outputValue = new IdFeatureWeightArrayWritable();

	@Override
	public void configure(JobConf job) { // change it to read from cache +
		threshold = job.getFloat(Config.THRESHOLD_PROPERTY, Config.THRESHOLD_VALUE);
		partitionsMaxes.add(new NormWeightSize());
		rPrefix = Integer.parseInt((new Path(job.get("map.input.file"))).getName());
		outputKey = new IntIntWritable(rPrefix);
		try {
			FileSystem hdfs = FileSystem.get(job);
			BufferedReader maxAllReader = new BufferedReader(new InputStreamReader(
					new DataInputStream(hdfs.open(new Path(job.get(Partitioner.MAX_DIR_PATH))))));
			String line;
			while ((line = maxAllReader.readLine()) != null) {
				StringTokenizer tkz = new StringTokenizer(line.replaceAll("[^,.0-9]", ""), ",");
				partitionsMaxes.add(new NormWeightSize(Float.parseFloat(tkz.nextToken()), Float
						.parseFloat(tkz.nextToken()), Integer.parseInt(tkz.nextToken())));
			}
			maxAllReader.close();
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
		int i;
		float thNorm = threshold / record.getNorm1();
		float thWeigth = threshold / record.getMaxWeight();
		double size = record.vectorSize;

		for (i = 1; i < rPrefix; i++) {
			if ((thNorm >= partitionsMaxes.get(i).maxWeight)
					|| (threshold / partitionsMaxes.get(i).maxNorm) >= record.getMaxWeight())
				continue;

			if ((thWeigth >= partitionsMaxes.get(i).maxNorm)
					|| (threshold / partitionsMaxes.get(i).maxWeight) >= record.getNorm1())
				continue;
			if ((size < (Math.pow(partitionsMaxes.get(i).maxWeight, 2)) || (partitionsMaxes.get(i).maxSize < Math
					.pow(thWeigth, 2))))
				continue;
			break;
		}
		return (i > 1) ? (i - 1) : rPrefix;
	}

	class NormWeightSize {
		float maxNorm;
		float maxWeight;
		int maxSize;

		public NormWeightSize() {}

		public NormWeightSize(float n, float w, int s) {
			maxNorm = n;
			maxWeight = w;
			maxSize = s;
		}

		@Override
		public String toString() {
			return (maxNorm + "," + maxWeight + "," + maxSize);
		}
	}
}
