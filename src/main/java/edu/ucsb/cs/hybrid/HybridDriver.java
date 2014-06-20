/**
 * Copyright 2012-2013 The Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on
 * an "AS IS"; BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under
 * the License.
 * 
 * Author: maha alabduljalil <maha (at) cs.ucsb.edu>
 * @Since Jul 26, 2012
 */

package edu.ucsb.cs.hybrid;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Date;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.ucsb.cs.hadoop.CustomSequenceFileInputFormat;
import edu.ucsb.cs.hadoop.NonSplitableSequenceInputFormat;
import edu.ucsb.cs.hybrid.io.TwoStageLoadbalancing;
import edu.ucsb.cs.hybrid.io.Splitter;
import edu.ucsb.cs.hybrid.mappers.IDMapper;
import edu.ucsb.cs.hybrid.mappers.PSS1_Mapper;
import edu.ucsb.cs.hybrid.mappers.PSS2_Mapper;
import edu.ucsb.cs.hybrid.mappers.MultipleS_Runner;
import edu.ucsb.cs.hybrid.mappers.PSS1_Threaded_Mapper;
import edu.ucsb.cs.hybrid.mappers.PSS_Bayardo_Mapper;
import edu.ucsb.cs.hybrid.mappers.PSS_Mapper;
import edu.ucsb.cs.hybrid.mappers.PSS2_SingleS_Mapper;
import edu.ucsb.cs.hybrid.mappers.PSS3_SingleS_Mapper;
import edu.ucsb.cs.hybrid.mappers.SingleS_Runner;
import edu.ucsb.cs.partitioning.PartDriver;
import edu.ucsb.cs.partitioning.jaccard.JaccardCoarsePartitionMain;
import edu.ucsb.cs.preprocessing.hashing.HashPagesDriver;
import edu.ucsb.cs.types.DocDocWritable;
import edu.ucsb.cs.utilities.JobSubmitter;

/*
 * IGNORE: block-1 doesn't have google dynamic.  
 */	
/**
 * The HybridDriver runs hybrid cosine similarity computations among documents of
 * the format: <code>KEY:docID , VALUE: feature weight</code>. It performs
 * comparisons within same the partition as if it was another partition. <br>
 * 
 * @see #run
 * 
 */
public class HybridDriver {

	private static String INPUT_DIR = PartDriver.OUTPUT_DIR;
	private static String OUTPUT_DIR = "similarityscores";

	public static void main(String args[]) throws ParseException, IOException {

		// job.set("mapred.job.tracker", "local");
		// job.set("fs.default.name", "file:///");

		JobConf job = new JobConf();
		job.setJarByClass(HybridDriver.class);
		new GenericOptionsParser(job, args);
		setMapperAndRunner(job);
		job.setMapOutputKeyClass(DocDocWritable.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(DocDocWritable.class);
		job.setOutputValueClass(FloatWritable.class);

		Path inputPath = new Path(INPUT_DIR);
		CustomSequenceFileInputFormat.addInputPath(job, inputPath);
		Path outputPath = new Path(OUTPUT_DIR);
		job.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, outputPath);
		FileSystem.get(job).delete(outputPath, true);

		job.setBoolean("fs.hdfs.impl.disable.cache", true); //xun not sure if needed

		if (job.getBoolean(Config.SPLITABLE_PROPERTY, Config.SPLITABLE_VALUE)) {
			job.setInputFormat(CustomSequenceFileInputFormat.class);
			Long splitMB = job.getLong(Config.SPLIT_MB_PROPERTY, Config.SPLIT_MB_VALUE) * 1024 * 1024;
			job.setLong("mapred.min.split.size", splitMB);
			job.setLong("mapred.max.split.size", splitMB);
			job.setLong("dfs.block.size", splitMB);
		} else {
			//  Comment the following of splitter for www experiments it assumes no splitting
			// of partitions for load balancing, should be fixed.
			Splitter.configure(job, inputPath);// remove comment unless for www
			job.setInputFormat(NonSplitableSequenceInputFormat.class); //remove comment
		}
		//SIGIR'14 two-stage balancing //not yet fully incorporated 
		if (job.getInt(Config.LOAD_BALANCE_PROPERTY, Config.LOAD_BALANCE_VALUE) != 0) {
			TwoStageLoadbalancing.main(job.getInt(Config.LOAD_BALANCE_PROPERTY, Config.LOAD_BALANCE_VALUE),
					new Path(PartDriver.OUTPUT_DIR), job);
		}
		JobSubmitter.run(job,"SIMILARITY",job.getFloat(Config.THRESHOLD_PROPERTY, Config.THRESHOLD_VALUE)); 
		if(job.getBoolean(Config.CONVERT_TEXT_PROPERTY, Config.CONVERT_TEXT_VALUE))
			IDMappingJob(args);
	}

	/**
	 * @param job : passed by reference to set its mapper class.
	 */
	public static void setMapperAndRunner(JobConf job) {
		int numSplits = job.getInt(Config.NUMBER_SPLITS_PROPERTY, Config.NUMBER_SPLITS_VALUE);
		int PSSChoice = job.getInt(Config.BLOCK_CHOICE_PROPERTY, Config.BLOCK_CHOICE_VALUE);//1,2
		String name = "PSS";
		if (numSplits>1) {
			//check can I set # splits for runner here?
			job.setMapRunnerClass(MultipleS_Runner.class);
			if (job.getBoolean(Config.MULTI_THREADS_PROPERTY, Config.MULTI_THREADS_VALUE)) { // threads testing
				job.setMapperClass(PSS1_Threaded_Mapper.class);// naming
			} else if (PSSChoice == 1){
				name += "1";
				job.setMapperClass(PSS1_Mapper.class);
			}else if (PSSChoice == 2){
				name += "2";
				job.setMapperClass(PSS2_Mapper.class);// MultipleS_Block1_Mapper
			}else
				;//For future implementations 
		} else {
			job.setMapRunnerClass(SingleS_Runner.class);
			if (job.getBoolean(Config.MULTI_THREADS_PROPERTY, Config.MULTI_THREADS_VALUE)) // threads
				throw new RuntimeException("ERROR: Single S with multithreads! Set hybrid.threads.property to false."); 
			if (PSSChoice == 1){
				job.setMapperClass(PSS_Mapper.class);
				if (job.getBoolean(Config.BAYADRO_SKIP_PROPERTY, Config.BAYADRO_SKIP_VALUE)){
					name += "/Bayardo_Dynamic_filter";
					job.setMapperClass(PSS_Bayardo_Mapper.class);//PSS+Bayardo WWW'07
				}
			}else if (PSSChoice == 2){
				name += "2/SingleS";
				job.setMapperClass(PSS2_SingleS_Mapper.class);
			}else
				job.setMapperClass(PSS3_SingleS_Mapper.class); //what is this?
		}
		job.setJobName(name);
	}

//	public static CommandLine setParser(Options options, String[] args) throws ParseException {
//		CommandLineParser parser = new PosixParser();
//		return (parser.parse(options, args));
//	}

	public static void run(JobConf job) throws IOException {
		String ret = stars() + "\n  Running job:  " + job.getJobName() + "\n  Input Path:   {";
		Path inputs[] = FileInputFormat.getInputPaths(job);
		for (int ctr = 0; ctr < inputs.length; ctr++) {
			if (ctr > 0) {
				ret += "\n                ";
			}
			ret += inputs[ctr].toString();
		}
		ret += "}\n";
		ret += "  Output Path:  " + FileOutputFormat.getOutputPath(job) + "\n";
		ret += "  Threshold:    " + job.getFloat(Config.THRESHOLD_PROPERTY, Config.THRESHOLD_VALUE)
				+ "\n";
		System.err.println(ret);

		Date startTime = new Date();
		JobClient.runJob(job);
		Date end_time = new Date();
		System.err.println("Similarity job took " + (end_time.getTime() - startTime.getTime())
				/ (float) 1000.0 + " seconds.");
	}

	public static String stars() {
		return new String(new char[77]).replace("\0", "*");
	}

	public static String IDS_FILE = "ids"; //this is alos copied to hybrid

	public static void IDMappingJob(String[] args) throws  IOException {

		JobConf job = new JobConf();
		new GenericOptionsParser(job, args);
		job.setJarByClass(HybridDriver.class);
		job.setJobName("Converting binary similarity scores to text");
		job.setMapperClass(IDMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		Path inputPath = new Path(OUTPUT_DIR);
		job.setInputFormat(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, inputPath);
		Path outputPath = new Path("SimilarityScores"); 
		job.setOutputFormat(TextOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, outputPath);
		FileSystem.get(job).delete(outputPath, true);
		HashPagesDriver.prepareDistribCache(job, HashPagesDriver.IDS_FILE2); //remove not sure
		JobSubmitter.run(job,"BINARY TO TEXT",job.getFloat(Config.THRESHOLD_PROPERTY, Config.THRESHOLD_VALUE)); 
	}
}
