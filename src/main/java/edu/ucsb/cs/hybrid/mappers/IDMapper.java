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
/**
 * 
 */
package edu.ucsb.cs.hybrid.mappers;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.ucsb.cs.hybrid.HybridDriver;
import edu.ucsb.cs.preprocessing.hashing.HashPagesDriver;
import edu.ucsb.cs.types.DocDocWritable;
import edu.ucsb.cs.types.FeatureWeightArrayWritable;

/**
 * @author maha alabdulajalil
 *
 */
public class IDMapper implements Mapper<DocDocWritable, FloatWritable, Text, Text> {

	/** Serial numbers mapping to Actual IDs */
	public HashMap<String,String> md5ToIdMap = new HashMap<String, String>();

	@Override
	public void configure(JobConf job) {
		readIdMappings(job,new Path(HashPagesDriver.IDS_FILE2)) ;
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
					StringTokenizer tkz = new StringTokenizer(strLine,": ");
					String key = tkz.nextToken();
					String value = tkz.nextToken();
					md5ToIdMap.put(key.replace(" ", ""), value.replace(" ", ""));
				}
				br.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() throws IOException {
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
	 */
	@Override
	public void map(DocDocWritable key, FloatWritable value,
			OutputCollector<Text, Text> output, Reporter reporter)
					throws IOException {
		StringBuilder bd = new StringBuilder(md5ToIdMap.get(String.valueOf(key.getDoc1()))+" "+md5ToIdMap.get(String.valueOf(key.getDoc2())));
		output.collect(new Text(bd.toString()), new Text(value.get()+""));
	}
}