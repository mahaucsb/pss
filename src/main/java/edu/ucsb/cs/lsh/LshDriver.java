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
 * @Since Jan 1, 2013
 */

package edu.ucsb.cs.lsh;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ProgramDriver;

import edu.ucsb.cs.lsh.minhash.MinHashLshDriver;
import edu.ucsb.cs.lsh.projection.ProjectionLshDriver;
import edu.ucsb.cs.lsh.statistics.LshStat;
import edu.ucsb.cs.utilities.JobSubmitter;

/**
 * @author Maha
 * 
 */
public class LshDriver {

	public static void main(String argv[]) {
		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();
		try {
			pgd.addClass("randomlsh", ProjectionLshDriver.class,
					"Partition input vectors according to random projections.");
			pgd.addClass("minhashlsh", MinHashLshDriver.class,
					"Partition input vectors according to minhash values.");
			pgd.addClass("lshstat", LshStat.class, "Collect statistics from binray lshpartitions/");
			pgd.driver(argv);
		} catch (Throwable e) {
			e.printStackTrace();
		}

		System.exit(exitCode);
	}
}
