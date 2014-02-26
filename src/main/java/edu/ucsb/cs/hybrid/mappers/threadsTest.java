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
 * @Since Dec 19, 2012
 */

package edu.ucsb.cs.hybrid.mappers;

/**
 * @author Maha
 * 
 */
public class threadsTest {
	int global = 0;

	public static void main(String[] args) {
		int numT = 10;
		threadsTest mama = new threadsTest();
		mythreads[] threads = new mythreads[numT];
		for (int i = 0; i < numT; i++) {
			threads[i] = mama.new mythreads(i);
			threads[i].start();
		}
		System.out.println("Waiting...");
	}

	class mythreads extends Thread {
		int tid;

		public mythreads(int i) {
			this.tid = i;
		}

		@Override
		public void run() {
			while (true) {
				System.out.println("run() for thread-" + this.tid);
				try {
					this.wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			// wait for others
		}
	}
}