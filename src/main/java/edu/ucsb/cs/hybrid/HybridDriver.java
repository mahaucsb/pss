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
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.ucsb.cs.hadoop.CustomSequenceFileInputFormat;
import edu.ucsb.cs.hadoop.NonSplitableSequenceInputFormat;
import edu.ucsb.cs.hybrid.io.Loadbalancing;
import edu.ucsb.cs.hybrid.io.Splitter;
import edu.ucsb.cs.hybrid.mappers.MultipleS_Block0_Mapper;
import edu.ucsb.cs.hybrid.mappers.MultipleS_Block11_Mapper;
import edu.ucsb.cs.hybrid.mappers.MultipleS_HybridRunner;
import edu.ucsb.cs.hybrid.mappers.MultipleS_Threaded_Block0_Mapper;
import edu.ucsb.cs.hybrid.mappers.SingleS_Block01_Mapper;
import edu.ucsb.cs.hybrid.mappers.SingleS_Block0_Mapper;
import edu.ucsb.cs.hybrid.mappers.SingleS_Block1_Mapper;
import edu.ucsb.cs.hybrid.mappers.SingleS_Block3_Mapper;
import edu.ucsb.cs.hybrid.mappers.SingleS_HybridRunner;
import edu.ucsb.cs.hybrid.types.DocDocWritable;
import edu.ucsb.cs.partitioning.PartDriver;

/*
 * Issue: block-1 doesn't have google dynamic.  
 */
/**
 * The HybridDriver runs hybrid cosine similarity computations among records of
 * the format: <code>KEY:docID , VALUE: feature weight</code>. It assumes
 * comparisons within same partition can be easily done, although in reality
 * this is hard with hadoop automatic splitting.(?) <br>
 * 
 * @see #run
 * 
 */
public class HybridDriver {

	// .partition.dot.vector
	private static String INPUT_DIR = PartDriver.OUTPUT_DIR;
	private static String OUTPUT_DIR = "hybridSim-results";

	public static void main(String args[]) throws ParseException, IOException {

		// job.set("mapred.job.tracker", "local");
		// job.set("fs.default.name", "file:///");

		JobConf job = new JobConf();
		job.setJarByClass(HybridDriver.class);
		unique((new GenericOptionsParser(job, args)).getRemainingArgs());

		setMapperAndRunner(job);
		job.setMapOutputKeyClass(DocDocWritable.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(DocDocWritable.class);
		job.setOutputValueClass(FloatWritable.class);

		String inputDir = INPUT_DIR;
		CustomSequenceFileInputFormat.addInputPath(job, new Path(inputDir));
		Path outputPath = new Path(OUTPUT_DIR);
		job.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, outputPath);
		FileSystem.get(job).delete(outputPath, true);

		if (job.getBoolean(Config.SPLITABLE_PROPERTY, Config.SPLITABLE_VALUE)) {
			job.setInputFormat(CustomSequenceFileInputFormat.class);
			Long splitMB = job.getLong(Config.SPLIT_MB_PROPERTY, Config.SPLIT_MB_VALUE) * 1024 * 1024;
			job.setLong("mapred.min.split.size", splitMB);
			job.setLong("mapred.max.split.size", splitMB);
			job.setLong("dfs.block.size", splitMB);
		} else {
			// The following comment of splitter is for www experiments it assumes no splitting
			// of partitions for load balancing, should be fixed.
			Splitter.configure(job, new Path(inputDir));// remove comment unless for www
			job.setInputFormat(NonSplitableSequenceInputFormat.class); //remove comment
		    ;
		}
		if (job.getInt(Config.LOAD_BALANCE_PROPERTY, Config.LOAD_BALANCE_VALUE) != 0) {
			Loadbalancing.main(job.getInt(Config.LOAD_BALANCE_PROPERTY, Config.LOAD_BALANCE_VALUE),
					new Path(PartDriver.OUTPUT_DIR), job);
		}
		run(job); 
	}

	public static JobConf prepareDistributedCache(JobConf job, String inputDir) {
		try {
			FileSystem hdfs = (new Path(inputDir)).getFileSystem(job);
			for (FileStatus path : hdfs.listStatus(new Path(inputDir)))
				if (hdfs.isFile(path.getPath()))
					DistributedCache.addCacheFile(path.getPath().toUri(), job);
			return job;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * @param job : passed by reference to set its mapper class.
	 */
	public static void setMapperAndRunner(JobConf job) {
		Boolean multipleS = job.getBoolean(Config.MULTIPLE_S_PROPERTY, Config.MULTIPLE_S_VALUE);
		int blockChoice = job.getInt(Config.BLOCK_CHOICE_PROPERTY, Config.BLOCK_CHOICE_VALUE);
		String name = "block-" + blockChoice + "/";
		if (multipleS) {
			job.setMapRunnerClass(MultipleS_HybridRunner.class);
			name += "multipleS-"
					+ job.getInt(Config.NUMBER_SPLITS_PROPERTY, Config.NUMBER_SPLITS_VALUE);
			if (job.getBoolean(Config.MULTI_THREADS_PROPERTY, Config.MULTI_THREADS_VALUE)) { // threads
				job.setMapperClass(MultipleS_Threaded_Block0_Mapper.class);// threads
			} else if (blockChoice == 0)
				job.setMapperClass(MultipleS_Block0_Mapper.class);
			else if (blockChoice == 1)
				job.setMapperClass(MultipleS_Block11_Mapper.class);// MultipleS_Block1_Mapper
			else
				;
		} else {// this should be removed
			name += "singleS";
			job.setMapRunnerClass(SingleS_HybridRunner.class);
			if (job.getBoolean(Config.BAYADRO_SKIP_PROPERTY, Config.BAYADRO_SKIP_VALUE)) // late
				job.setMapperClass(SingleS_Block01_Mapper.class);// late
			else if (job.getBoolean(Config.MULTI_THREADS_PROPERTY, Config.MULTI_THREADS_VALUE)) // threads
				throw new RuntimeException("Single S with multithreads ??!!!"); // threads
			else if (blockChoice == 0)
				job.setMapperClass(SingleS_Block0_Mapper.class);
			else if (blockChoice == 1)
				job.setMapperClass(SingleS_Block1_Mapper.class);// block11
			else
				job.setMapperClass(SingleS_Block3_Mapper.class);
		}
		job.setJobName(name);
	}

	public static CommandLine setParser(Options options, String[] args) throws ParseException {
		CommandLineParser parser = new PosixParser();
		return (parser.parse(options, args));
	}

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

	public static void unique(String[] args) throws UnsupportedEncodingException {
		if (args.length != 1)
			throw new UnsupportedEncodingException(
					"Usage: <className> -conf <confgs> <Unique Symbol>");
		INPUT_DIR += args[0];
		OUTPUT_DIR += args[0];
	}
}
