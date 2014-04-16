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
 * @Since Jul 27, 2012
 */

package edu.ucsb.cs.preprocessing.sequence;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.ucsb.cs.preprocessing.hashing.HashPagesDriver;
import edu.ucsb.cs.types.FeatureWeight;
import edu.ucsb.cs.types.FeatureWeightArrayWritable;

/**
 * <code>
 * input: 001 1 0.1 2 0.3 ... <br> 
 * output: MD5(001) 1 0.1 2 0.3 ...
 * </code>
 */
public class SeqMapper extends MapReduceBase implements
Mapper<Object, Text, LongWritable, FeatureWeightArrayWritable> {

	private LongWritable id = new LongWritable();
	private FeatureWeightArrayWritable vector = new FeatureWeightArrayWritable();
	/** Serial numbers mapping to Actual IDs */
	private HashMap<String,String> serialToIdMap = new HashMap<String, String>();
	private BufferedWriter br = null;

	@Override
	public void configure(JobConf job) {
		Path idPath = new Path(HashPagesDriver.IDS_FILE1);
		readIdMappings(job,idPath) ;
		openIdsMapping(HashPagesDriver.IDS_FILE2,job.get("mapred.task.partition"));
	}

	public void map(Object key, Text value,
			OutputCollector<LongWritable, FeatureWeightArrayWritable> output, Reporter reporter)
					throws IOException {
		setKey(value.toString());
		setValue(value.toString());
		output.collect(id, vector);
	}

	public void close(){
		closeIdsMapping();
	}

	/**
	 * Closes the text file holding the mapping md5 number "::" actual IDs per line to be used
	 * for converting back to original IDs
	 * @param job
	 * @param outputfile
	 */
	public void closeIdsMapping(){
		try{
			br.close();
		}catch (IOException e){
			System.err.println("Error: " + e.getMessage());
		}
	}

	/**
	 * This create a new file for writing the id mapping from md5 hash to 
	 * actual even if the file already exists.
	 * @param outputDir
	 * @param filename
	 */
	public void openIdsMapping(String outputDir, String filename){
		try{
			Path pt=new Path(outputDir+"/"+filename);
			FileSystem fs = FileSystem.get(new Configuration());
			br=new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
		}catch (IOException e){
			System.err.println("Error: " + e.getMessage());
		}
	}

	private void setKey(String record) throws IOException {
		StringTokenizer tkz = new StringTokenizer(record);
		String serialId = tkz.nextToken();
		long md5key = getMD5(serialId, 8);
		id.set(md5key); 
		br.write(md5key+" :: "+serialToIdMap.get(serialId)+"\n");
		serialToIdMap.remove(serialId);
	}

	private void setValue(String record) {
		StringTokenizer tkz = new StringTokenizer(record);
		tkz.nextToken();// id
		ArrayList<FeatureWeight> array = new ArrayList<FeatureWeight>();

		while (tkz.hasMoreTokens())
			array.add(new FeatureWeight(Long.parseLong(tkz.nextToken()), Float.parseFloat(tkz
					.nextToken())));
		vector.vectorSize = array.size();
		FeatureWeight[] toArray = new FeatureWeight[array.size()];
		array.toArray(toArray);
		vector.vector = toArray;
	}

	private static long getMD5(String s, int n) {
		byte[] result = new byte[n];
		try {
			byte[] bytesOfMessage = s.getBytes("UTF-8");
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.reset();
			md.update(bytesOfMessage);
			byte[] thedigest = md.digest(bytesOfMessage);
			for (int i = 0; i < n; i++)
				result[i] = thedigest[i];

			ByteArrayInputStream bos = new ByteArrayInputStream(result);
			DataInputStream dos = new DataInputStream(bos);
			return (dos.readLong());
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	public void readIdMappings(JobConf job, Path inputDir) {
		String strLine = null;
		try {
			FileSystem hdfs = FileSystem.get(job);
			if (!hdfs.exists(inputDir)) {
				throw new UnsupportedEncodingException("ERROR: "+inputDir.getName()+" doesn't exists in hdfs !");
			}
			FileStatus[] cachedFiles = hdfs.listStatus(inputDir);
			for(int i=0;i<cachedFiles.length;i++)
			{
				Path pt=  cachedFiles[i].getPath();
				BufferedReader br=new BufferedReader(new InputStreamReader(hdfs.open(pt)));
				while ((strLine = br.readLine()) != null)   {
					StringTokenizer tkz = new StringTokenizer(strLine,":: ");
					String key = tkz.nextToken();
					String value = tkz.nextToken();
					serialToIdMap.put(key.replace(" ", ""), value.replace(" ", ""));
				}
				br.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


}
