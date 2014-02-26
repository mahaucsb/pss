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
 * @Since Aug 29, 2012
 */

package edu.ucsb.cs.partitioning.statistics;

/**
 * to prepare Gnuplot datafile for plotting.
 * http://cacheonix.com/articles/Caching_for_Java_Applications.htm
 */
public class GijComparisons {

	public static void main(String[] args) {
		int n = 5;
		for (int i = 1; i <= n; i++)
			for (int j = 1; j <= i; j++)
				if (i == j) {
					System.out.println(i + "\t" + j + "\t"
							+ ((n - 1) + choose(j, 2) + (j - 1) * (n - j)));
				} else {
					System.out.println(i + "\t" + j + "\t"
							+ (choose(n + 1, 2) - choose(j + 1, 2) - 1));
				}
	}

	public static long choose(long n, int k) {
		if (k < 0 || k > n)
			return 0;
		if (k == 0 && n == 0)
			return 1;
		return choose(n - 1, k) + choose(n - 1, k - 1);
	}

}
