package edu.ucsb.cs.preprocessing.hashing;

/*
 * need to embed tf-idf weighting system if possible and only keep those bteween 0.2-0.85 like spotsig
 */
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.ucsb.cs.preprocessing.PreprocessDriver;

/**
 * Takes in a text files directory with each line representing a document as a
 * bag of words. It collects all the features and map each to a number. Then use
 * the hashing to hash each document. The output is still a bag/set of numeric
 * values representing the terms/features in text format plus a unique document
 * id for each line.<br>
 * Note: Choices (#3,#4,#6) are the only ones valid to be used for the sequence
 * conversion step that follows. <br>
 * 
 * ### Missing the MD5 is only used for optoin#3 make it for all later.
 * 
 * @author Maha Alabduljalil
 */
public class HashPagesDriver {

	public static final String NAMESPACE = "hashing";

	// this is useless
	public static final String MAX_FEATURE_FREQ_PROPERTY = NAMESPACE + ".max.freq";
	public static final int MAX_FEATURE_FREQ_VALUE = 1;
	/** Jimmy Lin idea of reducing computation time */
	public static final String DF_CUT_PROPERTY = NAMESPACE + ".df.cut";
	public static final float DF_CUT_VALUE = 0.01f;
	public static final String OPTION_NUMBER_PROPERTY = NAMESPACE + ".option.num";
	public static final int OPTION_NUMBER_VALUE = 3;
	public static final String NUM_REDUCERS_PROPERTY = NAMESPACE + ".num.reducers";
	public static final int NUM_REDUCERS_VALUE = 3;
	public static final String BINARY_WEIGHTS_PROPERTY = NAMESPACE + ".binary.weights";
	public static final boolean BINARY_WEIGHTS_VALUE = false;
	public static final String MD5_HASH_PROPERTY = NAMESPACE + ".md5.hash";
	public static final boolean MD5_HASH_VALUE = false;
	public static final String LONELY_FEATURES_PROPERTY = NAMESPACE + ".lonely.features";
	public static final boolean LONELY_FEATURES_VALUE = false;

	public static String INPUT_DIR = "textpages";
	private static String FEATURES_FILE = "features";
	public static String OUTPUT_DIR = "hashedvectors";

	public static void main(String[] args) throws IOException, ParseException {

		JobConf job = new JobConf();

		switch (configure(job, args)) {
			case 1:// collect corpse features
				collectFeatures(job);
				break;
			case 2:// hash features of vectors
				collectFeatures(job);
				hashFeatures(job, FeatureMapper.class);
				break;
			case 3:// hash features and assign weights.
				collectFeatures(job);
				hashFeatures(job, FeatureWeightMapper.class);
				break;
			case 4:// hash features and assign binary weights.
				collectFeatures(job);
				hashFeatures(job, FeatureBinaryWeightMapper.class);
				break;
			case 5:// hash words per vectors.
				collectFeatures(job);
				hashFeatures(job, NumericWordsMapper.class);
				break;
			case 6:// MD5 hash features with weights.
				hashFeatures(job, Md5FeatureWeightMapper.class);
				break;
			case 7:// MD5 hash features with binary weights.
				hashFeatures(job, Md5FeatureBinaryWeightMapper.class);
				break;
			default:
				break;
		}
	}

	/**
	 * Assigns a &ltunique symbol&gt to this job files in order to avoid
	 * accidentally deleting previous files.Then, find user choice of which job
	 * to run.
	 * @throws IOException
	 */
	public static int configure(JobConf job, String[] args) throws IOException {
		GenericOptionsParser gop = new GenericOptionsParser(job, args);
		args = gop.getRemainingArgs();
		if (args.length != 1)
			throw new UnsupportedEncodingException(
					"Please insert any unique <symbol> to avoid erasing old data.");
		INPUT_DIR = INPUT_DIR + args[0];
		OUTPUT_DIR = OUTPUT_DIR + args[0];
		FEATURES_FILE = FEATURES_FILE + args[0];
		if (job.getBoolean(BINARY_WEIGHTS_PROPERTY, BINARY_WEIGHTS_VALUE)
				&& !job.getBoolean(MD5_HASH_PROPERTY, MD5_HASH_VALUE))
			return 4;
		if (job.getBoolean(MD5_HASH_PROPERTY, MD5_HASH_VALUE)
				&& !job.getBoolean(BINARY_WEIGHTS_PROPERTY, BINARY_WEIGHTS_VALUE))
			return 6;
		if (job.getBoolean(MD5_HASH_PROPERTY, MD5_HASH_VALUE)
				&& job.getBoolean(BINARY_WEIGHTS_PROPERTY, BINARY_WEIGHTS_VALUE))
			return 7;
		return job.getInt(OPTION_NUMBER_PROPERTY, OPTION_NUMBER_VALUE);
	}

	/**
	 * Runs a MapReduce job to collect features of the input data but only those
	 * with frequency greater than 1.
	 */
	public static void collectFeatures(JobConf job) throws IOException {

		job.setJobName("Collect features");
		job.setJarByClass(HashPagesDriver.class);

		job.setMapperClass(InvertedIndexMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(job.getInt(NUM_REDUCERS_PROPERTY, NUM_REDUCERS_VALUE));
		job.setReducerClass(InvertedIndexReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		TextInputFormat.addInputPath(job, new Path(INPUT_DIR));
		Path outputPath = new Path(FEATURES_FILE);
		FileSystem.get(job).delete(outputPath, true);
		FileOutputFormat.setOutputPath(job, outputPath);
		PreprocessDriver.run(job);
	}

	/**
	 * Runs a job with maps only to hash the input directory of records into
	 * numeric values. It assumes a file with features already exists in HDFS.
	 * Unless the choice was to use MD5 hashing.
	 */
	public static void hashFeatures(JobConf job, Class mapper) throws IOException {
		job.setJobName("Hash pages using " + mapper.getSimpleName());
		job.setJarByClass(HashPagesDriver.class);
		if (!mapper.getName().contains("Md5"))
			prepareDistribCache(job, FEATURES_FILE);
		job.setMapperClass(mapper);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		TextInputFormat.setInputPaths(job, new Path(INPUT_DIR));
		Path outputPath = new Path(OUTPUT_DIR);
		FileSystem.get(job).delete(outputPath, true);
		FileOutputFormat.setOutputPath(job, outputPath);
		PreprocessDriver.run(job);
	}

	/**
	 * Adds all the files from the parameter directory to this job
	 * distributedCache.
	 */
	public static void prepareDistribCache(JobConf job, String directory) {
		try {

			Path dir = new Path(directory);
			FileSystem hdfs = FileSystem.get(job);
			for (FileStatus child : hdfs.listStatus(dir))
				if (!child.getPath().getName().contains("_"))
					DistributedCache.addCacheFile(child.getPath().toUri(), job);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}