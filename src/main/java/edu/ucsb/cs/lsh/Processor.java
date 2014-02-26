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
 * @Since Sep 4, 2012
 */

package edu.ucsb.cs.lsh;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * 
 * This is a read/write directory to another directory after removing the
 * weights from lines. Not needed if lines are already bag of words.
 * 
 * @author Maha
 */
public class Processor {

	public static void main(String[] args) throws IOException {
		System.out.println("Usage: <inputDir> <outputDir>");

		File inputDir = new File("/Users/Hadoop/data/clueweb/1m/");
		if (!inputDir.exists())
			return;
		String outDir = "/Users/Hadoop/cpc/cpc-approximate/data/";
		long fcount = -1, lcount = 0;

		for (File f : inputDir.listFiles()) {
			BufferedWriter out = new BufferedWriter(new FileWriter(outDir + "/" + f.getName()));
			fcount++;
			System.out.println("Reading " + f.getName());
			DataInputStream in = new DataInputStream(new FileInputStream(f.getPath()));
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			while ((strLine = br.readLine()) != null) {
				lcount++;
				out.write(fcount + "" + lcount + " " + strLine + "\n");
			}
			in.close();
			out.write("\n");
			out.close();
		}
	}
}
