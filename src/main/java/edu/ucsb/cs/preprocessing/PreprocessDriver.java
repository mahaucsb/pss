package edu.ucsb.cs.preprocessing;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ProgramDriver;

import edu.ucsb.cs.preprocessing.cleaning.CleanPagesDriver;
import edu.ucsb.cs.preprocessing.cleaning.WarcFileCleaner;
import edu.ucsb.cs.preprocessing.hashing.HashPagesDriver;
import edu.ucsb.cs.preprocessing.sequence.SeqFilesCombiner;
import edu.ucsb.cs.preprocessing.sequence.SeqWriter;
import edu.ucsb.cs.preprocessing.sequence.SeqReader;
import edu.ucsb.cs.preprocessing.sequence.SequenceDriver;

/**
 * Starts data preprocessing including: html cleaning, hashing, converting to
 * Hadoop sequence and reading the sequence files.
 */
public class PreprocessDriver {

	/**
	 * Prints these options to chose from:<br>
	 * - [clean] documents to produce document ID: bag of cleaned words. <br>
	 * - [hash] bag of words into bag of hashed tokens.<br>
	 * - Produce [sequence] records [LongWritable,FeatureWeightArrayWritable] <br>
	 * - [seq] deals with writing/reading/combining sequence files.
	 * 
	 * @param argv : command line inputs
	 */
	public static void main(String argv[]) {
		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();
		try {
			pgd.addClass("clean", CleanPagesDriver.class,
					"A MapReduce job to clean input pages. See options.");
			pgd.addClass(
					"hash", HashPagesDriver.class,
					"A MapReduce job to collect features then hash input data into [docId <features>] with associated weights if desired. ");
			pgd.addClass("seq", SequenceDriver.class,
					"For writing/reading/merging sequence files. See optoins.\n\n");
			pgd.driver(argv);
		} catch (Throwable e) {
			e.printStackTrace();
		}

		System.exit(exitCode);
	}
}
