package edu.ucsb.cs.preprocessing.cleaning;

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
 * @Since Aug 4, 2012
 */

/**
 * @author Maha
 * This class prepares input into DBLP format
 */
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

/*
 */
public class YahooDataConverter {
	public static void main(String args[]) {
		try {
			if (args.length == 0) {
				System.out
						.println("Usage: <inputfile of vectors> <outputfile> will produce vectors as bag of music.");
				System.exit(2);
			}
			DataInputStream in = new DataInputStream(new FileInputStream(args[0]));
			// "/Users/Hadoop/data/emails/AllEmails"));
			BufferedWriter out = new BufferedWriter(new FileWriter(args[1]));
			// "/Users/Hadoop/cpc/cpc-apss/data/emails/AllEmails-bags"));

			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			while ((strLine = br.readLine()) != null) {
				StringTokenizer tkz = new StringTokenizer(strLine, "|");
				tkz.nextToken();
				int numSongs = Integer.parseInt(tkz.nextToken());
				for (int i = 0; i < numSongs; i++) {
					strLine = br.readLine();
					tkz = new StringTokenizer(strLine, " \t");
					// out.write(title.nextToken().replaceAll("[^A-Za-z0-9]",
					// "") + " ");
					out.write(tkz.nextToken() + " ");
				}
				out.write("\n");
			}
			in.close();
			out.close();
		} catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
		}
	}
}