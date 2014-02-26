package edu.ucsb.cs.hybrid.io;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

/**
 * This is for cache stuff measurement, it might not produce correct results.
 * @author Maha
 * 
 */
public class OneMapReader extends Reader {

	/**
	 * @param job
	 * @param inputPath: path with one file for one map task.
	 * @param ioBlockSize: block of vectors to be read from others at a time.
	 */
	public OneMapReader(JobConf job, Path inputPath, int blockSize) throws IOException {
		super(job, inputPath, blockSize);
	}

	/**
	 * @param inputPath :changed here to "others" which is a different directory
	 *        than one map file.
	 */
 	@Override
	public void getFiles(JobConf job, Path inputPath) {
		inputPath = new Path(Splitter.OTHERS_INPUT);
		super.getFiles(job, inputPath);
	}
}
