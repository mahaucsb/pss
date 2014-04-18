package edu.ucsb.cs.preprocessing.hashing;

/*
 * need to embed tf-idf weighting system if possible and only keep those bteween 0.2-0.85 like spotsig
 */
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
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

import edu.ucsb.cs.preprocessing.Config;
import edu.ucsb.cs.preprocessing.PreprocessDriver;
import edu.ucsb.cs.utilities.JobSubmitter;

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


	public static String INPUT_DIR = "textpages";
	private static String FEATURES_FILE = "features";
	public static String IDS_FILE1 = "serial-ids-mappings";
	public static String IDS_FILE2 = "md5-ids-mappings";
	public static String OUTPUT_DIR = "hashedvectors";

	/** Actual IDs to serial numbers mapping */
	public static HashMap<String,String> idHash = new HashMap<String, String>();

	public static void main(String[] args) throws IOException, ParseException {

		JobConf job = new JobConf();

		//Different ways to hash input but (3) is default for PSS.
		switch (configure(job, args)) {
		case 1:// collect corpse features
			collectFeatures(job);
			break;
		case 2:// hash features of vectors
			collectFeatures(job);
			hashFeatures(job, FeatureMapper.class);
			break;
		case 3:// hash features and assign weights.
			collectFeatures(job); //remove comment
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

		if (job.getBoolean(Config.BINARY_WEIGHTS_PROPERTY, Config.BINARY_WEIGHTS_VALUE)
				&& !job.getBoolean(Config.MD5_HASH_PROPERTY, Config.MD5_HASH_VALUE))
			return 4;
		if (job.getBoolean(Config.MD5_HASH_PROPERTY, Config.MD5_HASH_VALUE)
				&& !job.getBoolean(Config.BINARY_WEIGHTS_PROPERTY, Config.BINARY_WEIGHTS_VALUE))
			return 6;
		if (job.getBoolean(Config.MD5_HASH_PROPERTY, Config.MD5_HASH_VALUE)
				&& job.getBoolean(Config.BINARY_WEIGHTS_PROPERTY, Config.BINARY_WEIGHTS_VALUE))
			return 7;
		return job.getInt(Config.OPTION_NUMBER_PROPERTY, Config.OPTION_NUMBER_VALUE);
	}

	/**
	 * Runs a MapReduce job to collect features of the input data but only those
	 * with frequency greater than 1 if @see Config.LONELY_FEATURES_PROPERTY is enabled.
	 */
	public static void collectFeatures(JobConf job) throws IOException {

		job.setJobName("Collect features");
		job.setJarByClass(HashPagesDriver.class);

		job.setMapperClass(InvertedIndexMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(job.getInt(Config.NUM_REDUCERS_PROPERTY, Config.NUM_REDUCERS_VALUE));
		job.setReducerClass(InvertedIndexReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		TextInputFormat.addInputPath(job, new Path(INPUT_DIR));
		Path outputPath = new Path(FEATURES_FILE);
		FileSystem.get(job).delete(outputPath, true);
		FileOutputFormat.setOutputPath(job, outputPath);
		JobSubmitter.run(job,"PREPROCESS",-1);
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
		FileSystem.get(job).delete(new Path(IDS_FILE1), true);
		JobSubmitter.run(job,"PREPROCESS",-1);
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