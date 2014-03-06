package edu.ucsb.cs.invertedindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.ucsb.cs.hybrid.HybridDriver;
import edu.ucsb.cs.invertedindex.mappers.InvertedMapper;
import edu.ucsb.cs.invertedindex.mappers.InvertedSimMapper;
import edu.ucsb.cs.invertedindex.reducers.InvertedReducer;
import edu.ucsb.cs.invertedindex.reducers.InvertedSimReducer;
import edu.ucsb.cs.types.DocDocWritable;
import edu.ucsb.cs.types.DocWeight;
import edu.ucsb.cs.types.DocWeightArrayWritable;

/**
 * This class performs the APSS using the inverted index format for inputs and
 * data in memory. It first builds the inverted index from a sequence input file
 * of the format:
 * 
 * <pre>
 * KEY: LongWritable as document ID
 * VALUE: FeatureWeightArrayWritable as vector of tokens and their weights.
 * </pre>
 * 
 * The output is the similarity results of (d_i,d_j,score) also in sequence
 * format:
 * 
 * <pre>
 * KEY: DocDocWritable
 * VALUE: FloatWritable.
 * </pre>
 */
public class InvertedIndexDriver {

	public static void printUsage() {
		System.out.println("Usage: [GenericOptions] <inputSeqDir> <outputDir>");
		System.exit(1);
	}

	public static void main(String[] args) throws Exception {
		JobConf job = new JobConf();
		job.setJobName("InvertedIndexDriver-BuildII");
		job.setJarByClass(InvertedIndexDriver.class);
		GenericOptionsParser gop = new GenericOptionsParser(job, args);
		args = gop.getRemainingArgs();

		if (args.length != 2)
			printUsage();
		//
		// Job1
		//

		job.setMapperClass(InvertedMapper.class);
		job.setReducerClass(InvertedReducer.class);
		job.setNumReduceTasks(4);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(DocWeight.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(DocWeightArrayWritable.class);

		job.setInputFormat(SequenceFileInputFormat.class);
		SequenceFileInputFormat.addInputPath(job, new Path(args[0]));
		job.setOutputFormat(SequenceFileOutputFormat.class);
		Path interPath = new Path("inverted");
		FileSystem.get(job).delete(interPath, true);
		SequenceFileOutputFormat.setOutputPath(job, interPath);

		HybridDriver.run(job);

		//
		// Collect statistics
		//

		//
		// Job2
		//
		job = new JobConf(new Configuration());
		job.setJarByClass(InvertedIndexDriver.class);
		job.setJobName("InvertedIndexDriver-Similarity (SII)");
		job.setMapperClass(InvertedSimMapper.class);
		job.setReducerClass(InvertedSimReducer.class);
		job.setNumReduceTasks(5);
		job.setInputFormat(SequenceFileInputFormat.class);
		SequenceFileInputFormat.addInputPath(job, new Path("inverted"));

		job.setOutputFormat(SequenceFileOutputFormat.class);
		Path outputPath = new Path(args[1]);
		FileSystem.get(job).delete(outputPath, true);
		SequenceFileOutputFormat.setOutputPath(job, outputPath);

		job.setOutputKeyClass(DocDocWritable.class);
		job.setOutputValueClass(FloatWritable.class);
		long t = System.currentTimeMillis();
		HybridDriver.run(job);
		System.out.println("Job took " + (System.currentTimeMillis() - t) + " millisec.");

	}
}