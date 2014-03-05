package edu.ucsb.cs.hadoop;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.*;
import java.io.IOException;

public class NonSplitableSequenceInputFormat extends SequenceFileInputFormat{

	private static Path path;
	private static long SplitLen;
	private static JobConf j;

	public static FileSplit getSplit(){
		return new FileSplit(path,0, SplitLen, j) ;
	}
	@Override
	public RecordReader getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException {

		SplitLen = split.getLength();
		FileSplit f = (FileSplit)split; 
		path = f.getPath();
		j=job;
		
		return new SequenceFileRecordReader(job, (FileSplit) split);
	}	

	@Override
	public boolean isSplitable(FileSystem fs, Path filename){
		return false;
	}
}
