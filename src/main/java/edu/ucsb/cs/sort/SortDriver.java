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
 * @Since Oct 18, 2012
 */

package edu.ucsb.cs.sort;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Date;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ProgramDriver;

import edu.ucsb.cs.preprocessing.sequence.SeqWriter;
import edu.ucsb.cs.sort.length.LengthSortMain;
import edu.ucsb.cs.sort.maxw.MaxwSortMain;
import edu.ucsb.cs.sort.norm.NormSortMain;
import edu.ucsb.cs.sort.signature.SigSortMain;

/**
 * @author Maha
 * 
 */
public class SortDriver {
	public static final String NAMESPACE = "sort";
	public static final String NUM_REDUCE_PROPERTY = NAMESPACE + ".num.reducers";
	public static final int NUM_REDUCE_VALUE = 2;

	public static String INPUT_DIR = SeqWriter.OUTPUT_DIR;
	public static String OUTPUT_DIR = "sortedvectors";

	public static void main(String args[]) throws UnsupportedEncodingException {

		ProgramDriver pgd = new ProgramDriver();
		try {
			pgd.addClass("lengthsort", LengthSortMain.class, "\tSort documents based on length");
			pgd.addClass("normsort", NormSortMain.class, "\tSort documents based on p-norm");
			pgd.addClass("maxwsort", MaxwSortMain.class, "\tSort documents based on max weight");// del
			pgd.addClass("sigsort", SigSortMain.class, "\tSort documents based on their signatures");
			pgd.driver(args);
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	public static void runJob(String choice, String[] args) throws UnsupportedEncodingException {
		String[] argv = new String[args.length+1];
		argv[0] = choice;
		for (int i = 0; i < args.length; i++)
			argv[i + 1] = args[i];
		main(argv);
	}
}
