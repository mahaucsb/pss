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
 * @Since Sep 2, 2012
 */

package edu.ucsb.cs.hybrid.statistics;

import java.io.FileWriter;
import java.io.IOException;

/**
 * @author Maha
 * 
 */
public class CvsWriter {

	public static void main(String[] args) {
		writecsv(args[0]);
	}

	public static void writecsv(String filePath) {
		try {
			FileWriter writer = new FileWriter(filePath);
			for (int i = 1; i < 10; i++) {
				for (int j = 0; j < i * 1.2; j++) {
					writer.append(j + "");
					if (j < i * 1.2 - 1) {
						writer.append(',');
					} else {
						writer.append('\n');
					}
				}
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
