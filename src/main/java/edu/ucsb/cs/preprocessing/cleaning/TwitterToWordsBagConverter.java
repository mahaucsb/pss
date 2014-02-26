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

import edu.ucsb.cs.preprocessing.hashing.Md5FeatureWeightMapper;

/*
 * To convert Twitter dataset produced by Alex or emails into bag of words.  
 * Input: [ Docid <feature,freq>+ ]+ whether feature is hashed or not and repeats words based on frequency.  
 */
public class TwitterToWordsBagConverter {
	public static void main(String args[]) {
		try {
			if (args.length != 2) {
				System.out
						.println("Usage: <inputfile of vectors> <outputfile> will convert file[Id <feature,freq> into bag of words.");
				System.exit(2);
			}
			DataInputStream in = new DataInputStream(new FileInputStream(args[0]));
			// "/Users/Hadoop/data/emails/AllEmails"));
			BufferedWriter out = new BufferedWriter(new FileWriter(args[1]));
			// "/Users/Hadoop/cpc/cpc-apss/data/emails/AllEmails-bags"));

			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			while ((strLine = br.readLine()) != null)
				out.write(getBag(strLine) + "\n");
			// out.write(getVector(strLine) + "\n");
			// out.write(hashGetVector(strLine) + "\n");
			in.close();
			out.close();
		} catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
		}
	}

	/**
	 * @param vector: id [term,freq]+
	 * @return normalized vector : id [term,weight]+
	 */
	public static String getVector(String vector) {
		float sum = getSqrtSumSquares(vector);
		StringBuilder bdr = new StringBuilder();
		StringTokenizer tkz = new StringTokenizer(vector);
		bdr.append(tkz.nextToken() + " "); // id
		while (tkz.hasMoreTokens()) {
			bdr.append(tkz.nextToken() + " "); // term
			float freq = Float.parseFloat(tkz.nextToken());
			bdr.append((freq / sum) + " ");
		}
		return bdr.toString();
	}

	/**
	 * @param vector: id [term,freq]+
	 * @return bag vector : term1 term1 term2 ....
	 */

	public static String getBag(String vector) {
		StringBuilder bdr = new StringBuilder();
		StringTokenizer tkz = new StringTokenizer(vector);
		tkz.nextToken(); // skip id
		while (tkz.hasMoreTokens()) {
			String feature = tkz.nextToken();
			int freq = Integer.parseInt(tkz.nextToken());
			for (int i = 0; i < freq; i++)
				bdr.append(feature + " "); // term
		}
		return bdr.toString();
	}

	/**
	 * @param vector: id [term,freq]+
	 * @return normalized and hash vector : id [MD5(term),weight]+
	 */
	public static String hashGetVector(String vector) {
		float sum = getSqrtSumSquares(vector);
		StringBuilder bdr = new StringBuilder();
		StringTokenizer tkz = new StringTokenizer(vector);
		bdr.append(tkz.nextToken() + " "); // id
		while (tkz.hasMoreTokens()) {
			bdr.append(Md5FeatureWeightMapper.getMD5(tkz.nextToken(), 8) + " "); // term
			float freq = Float.parseFloat(tkz.nextToken());
			bdr.append((freq / sum) + " ");
		}
		return bdr.toString();
	}

	public static float getSqrtSumSquares(String vector) {
		float sum = 0;
		StringTokenizer tkz = new StringTokenizer(vector);
		tkz.nextToken(); // id
		while (tkz.hasMoreTokens()) {
			tkz.nextToken(); // term
			float freq = Float.parseFloat(tkz.nextToken()); // freq
			sum += (freq * freq);
		}
		return (float) Math.sqrt(sum);
	}
}