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

package edu.ucsb.cs.utilities;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

/**
 * @author Maha
 * 
 */
public class JobSubmitter {

	public static void run(JobConf job) {

		String ret = stars() + "\n  Running job:  " + job.getJobName() + "\n  Input Path:   {";
		Path inputs[] = FileInputFormat.getInputPaths(job);
		for (int ctr = 0; ctr < inputs.length; ctr++) {
			if (ctr > 0) {
				ret += "\n                ";
			}
			ret += inputs[ctr].toString();
		}
		ret += "}\n";
		ret += "  Output Path:  " + FileOutputFormat.getOutputPath(job) + "\n"
				+ "  Number of mappers:  " + job.getNumMapTasks() + "\n"
				+ "  Number of reducers:  " + job.getNumReduceTasks() + "\n";
		for (int ctr = 0; ctr < Properties.requiredParameters.size(); ctr++)
			ret += Properties.requiredParameters.get(ctr) + "\n";
		System.out.println(ret);
		try {
			JobClient.runJob(job);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static String stars() {
		return new String(new char[77]).replace("\0", "*");
	}

}
