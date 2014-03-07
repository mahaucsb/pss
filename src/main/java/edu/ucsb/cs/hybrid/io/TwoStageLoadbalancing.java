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
 * @Since Sep 11, 2013
 */

package edu.ucsb.cs.hybrid.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

import edu.ucsb.cs.hybrid.Config;
import edu.ucsb.cs.partitioning.statistics.Collector;
import edu.ucsb.cs.types.TextArrayWritable;

/**
 * @author Maha Load balancing stages as described by WWW'14 error: should be
 *         applied after cutting partitions into s :( but it's not.
 */
public class TwoStageLoadbalancing {

	public static Path partitionComparisonList = new Path("partitionCompList");
	private static ArrayList<String> partitionsNames = new ArrayList<String>();
	private static HashMap<String, Long> partitionsSizes = new HashMap<String, Long>();
	private static HashMap<String, Long> partitionsWeights = new HashMap<String, Long>();
	private static HashMap<String, Long> partitionsCircularWeights = new HashMap<String, Long>();
	private static HashMap<String, ArrayList<String>> undirectedGraph = new HashMap<String, ArrayList<String>>();
	private static HashMap<String, ArrayList<String>> directedGraph = new HashMap<String, ArrayList<String>>();
	private static boolean debugStages;
	private int step = 0;

	public static void main(int step, Path inputDir, JobConf job) throws IOException {
		FileSystem hdfs = inputDir.getFileSystem(job);
		if (!hdfs.exists(Collector.partitionSizesPath)) {
			System.out.println("Partition sizes file does not exists!");
			return;
		}
		debugStages = job.getBoolean(Config.DEBUG_STAGES_PROPERTY, Config.DEBUG_STAGES_VALUE);
		MapFile.Reader partitionSizeReader = new MapFile.Reader(hdfs,
				Collector.partitionSizesPath.getName(), new JobConf());
		Text partitionK = new Text();
		LongWritable partSizeV = new LongWritable();

		try {
			while (partitionSizeReader.next(partitionK, partSizeV)) {
				partitionsNames.add(partitionK.toString()); // useless?
				partitionsSizes.put(partitionK.toString(), partSizeV.get());
			}
		} catch (Exception e) {
			;
		}
		for (int i = 0; i < partitionsNames.size(); i++) {
			System.out.println("Partition " + partitionsNames.get(i) + " has "
					+ partitionsSizes.get(partitionsNames.get(i)) + " vectors.");
		}

		if (partitionsNames.size() <= 1)
			return;
		stage0();
		printUndirectedNeighbors("Stage0");
		printPartitionsStat("Stage0");

		printCircularPartitionsWeight("\nCircular");
		calcCWStandardDeviation();

		stage1();
		printDirectedNeighbors("Stage1");
		System.out.println("Stage 1 final weights: ");
		printPartitionsWeights("Stage1");
		if ((step == 2) || (step == 12)) {
			stage2();
			printDirectedNeighbors("Stage2");
			System.out.println("Stage 2 final weights: ");
			printPartitionsWeights("Stage2");
		}
		// stage3(job, hdfs);
		writeComparisonList(job, hdfs);
		// printComparisonList(job, hdfs);// remove
	}

	public static void stage0() {
		System.out.println("\nStage0:");
		initializePartitionsWeight();
		System.out.println();
		for (int i = 0; i < partitionsNames.size(); i++)
			System.out.println(partitionsNames.get(i) + " Undirected weight is: "
					+ partitionsWeights.get(partitionsNames.get(i)));
	}

	public static void stage1() {
		String minWeightPart;
		System.out.println("\nStage1:---------------------");
		while ((minWeightPart = findMinWeight()) != null) {
			updateUndirectedGraph(minWeightPart);
			System.out.println("Stage1 minWeightPartition : " + minWeightPart);
			printPartitionsStat("Stage1");
		}
		// calculatePartitionsCost();//buggy
	}

	public static void stage2() {
		System.out.println("\nStage2:---------------------");
		String maxCostPart;
		do {
			maxCostPart = findMaxCost();
			System.out.println("Stage2 maxWeightPartition : " + maxCostPart);
		} while (updateDirectedGraph(maxCostPart));
		System.out.println("Stage2 second round:---------------------");
		for (int i = 0; i < partitionsNames.size(); i++) {
			maxCostPart = partitionsNames.get(i);
			System.out.println("Stage2 maxWeightPartition : " + maxCostPart);
			updateDirectedGraph(maxCostPart);
		}
		// calculatePartitionsCost();
	}

	public static void printPartitionsWeights(String stage) {
		for (int i = 0; i < partitionsNames.size(); i++) {
			String currentPartition = partitionsNames.get(i);
			System.out.println(currentPartition + " weight: "
					+ partitionsWeights.get(currentPartition));
		}
	}

	public static void printPartitionsStat(String stage) {
		ArrayList<String> neighbours;
		long pw = 0, me, other, max = 0;
		double avg = 0, sd = 0;
		for (int i = 0; i < partitionsNames.size(); i++) {
			String currentPartition = partitionsNames.get(i);
			avg += partitionsWeights.get(currentPartition);
			if (max < partitionsWeights.get(currentPartition))
				max = partitionsWeights.get(currentPartition);
		}
		avg = avg / partitionsNames.size();
		System.out.println(stage + " trend max:" + max + ", avg:" + avg + ", stdev:"
				+ calcWStandardDeviation(avg));
	}

	public static void printCircularPartitionsWeight(String stage) {
		long pw = 0, me, other, nPart = partitionsNames.size();
		long combin = (nPart * (nPart - 1) / 2);
		System.out.println(stage + " partition weights:");
		for (int i = 0; i < nPart; i++) {
			String currentPartition = partitionsNames.get(i);
			System.out.print(currentPartition + ": ");
			int myRow = Reader.getRow(currentPartition);
			int myCol = Reader.getCol(currentPartition);
			int mapId = i;
			me = partitionsSizes.get(currentPartition);
			pw = me * me;
			for (int j = mapId + 1; j <= mapId + (combin / nPart); j++) {
				String otherPartition = partitionsNames.get((int) (j % nPart));
				int otherRow = Reader.getRow(otherPartition);
				int otherCol = Reader.getCol(otherPartition);
				if ((((myRow != myCol) && (myCol >= otherRow)) || ((otherRow != otherCol) && (otherCol >= myRow)))
						|| otherPartition.equals(currentPartition))
					continue;
				other = partitionsSizes.get(partitionsNames.get((int) (j % nPart)));
				pw += (me * other);
			}
			if (mapId < (combin % nPart)) {
				String otherPartition = partitionsNames
						.get((int) ((mapId + (combin / nPart) + 1) % nPart));
				int otherRow = Reader.getRow(otherPartition);
				int otherCol = Reader.getCol(otherPartition);
				if (!((((myRow != myCol) && (myCol >= otherRow)) || ((otherRow != otherCol) && (otherCol >= myRow))) || otherPartition
						.equals(currentPartition))) {
					other = partitionsSizes.get(partitionsNames.get((int) ((mapId
							+ (combin / nPart) + 1) % nPart))); // buggy?
					pw += (me * other);
				}
			}
			System.out.println(pw);
			partitionsCircularWeights.put(currentPartition, pw);
		}
	}

	public static void stage3(JobConf job, FileSystem hdfs) throws IOException {}

	public static void printDirectedNeighbors(String stage) {
		System.out.println(stage + " neighbors list:");
		for (int i = 0; i < partitionsNames.size(); i++) {
			ArrayList<String> neighbours = directedGraph.get(partitionsNames.get(i));
			System.out.print("Parition " + partitionsNames.get(i) + " direct neighbors are: ");
			for (int j = 0; j < neighbours.size(); j++) {
				System.out.print(neighbours.get(j) + ", ");
			}
			System.out.println();
		}
	}

	public static void printUndirectedNeighbors(String stage) {
		System.out.println(stage + " neighbors list:");
		for (int i = 0; i < partitionsNames.size(); i++) {
			ArrayList<String> neighbours = undirectedGraph.get(partitionsNames.get(i));
			System.out.print("Parition " + partitionsNames.get(i) + " undirect neighbors are: ");
			for (int j = 0; j < neighbours.size(); j++) {
				System.out.print(neighbours.get(j) + ", ");
			}
			System.out.println();
		}
	}

	public static void writeComparisonList(JobConf job, FileSystem hdfs) throws IOException {
		if (hdfs.exists(partitionComparisonList))
			hdfs.delete(partitionComparisonList);
		MapFile.Writer partCompListWriter = new MapFile.Writer(job, hdfs,
				partitionComparisonList.getName(), Text.class, TextArrayWritable.class);
		for (int i = 0; i < partitionsNames.size(); i++) {
			ArrayList<String> neighbors = directedGraph.get(partitionsNames.get(i));
			if (neighbors != null) { // is this needed?
				Text[] toCompare = new Text[neighbors.size()];
				for (int j = 0; j < toCompare.length; j++)
					toCompare[j] = new Text(neighbors.get(j));
				partCompListWriter.append(new Text(partitionsNames.get(i)), new TextArrayWritable(
						toCompare));
			}
		}
		partCompListWriter.close();
	}

	public static void printComparisonList(JobConf job, FileSystem hdfs) throws IOException {
		MapFile.Reader partCompListReader = new MapFile.Reader(hdfs,
				partitionComparisonList.getName(), job);
		Text part = new Text();
		TextArrayWritable array = new TextArrayWritable();
		partCompListReader.get(part, array);
		System.out.println(part.toString() + " neighbors: ");
		for (int i = 0; i < array.get().length; i++)
			System.out.print(array.get()[i] + ", ");

	}

	/**
	 * Stage 2 tool
	 * @return partition with maximum cost using the directed graph.
	 */
	public static String findMaxCost() {
		long maxC = 0;
		String minPart = null;
		Iterator<String> itr = partitionsWeights.keySet().iterator();
		while (itr.hasNext()) {
			String part = itr.next();
			long c = partitionsWeights.get(part);
			if (c > maxC) {
				maxC = c;
				minPart = part;
			}
		}
		return minPart;
	}

	public static String getMinCostNeighbour(ArrayList<String> directedNeighbors) {
		long c = Long.MAX_VALUE;
		String minCostPart = null;
		if (directedNeighbors != null)
			for (int i = 0; i < directedNeighbors.size(); i++) {
				String part = directedNeighbors.get(i);
				long partCost = partitionsWeights.get(part);
				if (c > partCost) {
					minCostPart = part;
					c = partCost;
				}
			}
		return minCostPart;
	}

	public static boolean inspectFlipping(String maxCostPart, String minCostNeighbor) {
		long maxPartW = partitionsWeights.get(maxCostPart);
		long minNeighborW = partitionsWeights.get(minCostNeighbor);
		long maxPartSize = partitionsSizes.get(maxCostPart);
		long minNeighborSize = partitionsSizes.get(minCostNeighbor);
		if ((minNeighborW + (maxPartSize * minNeighborSize)) < maxPartW) {
			partitionsWeights.put(maxCostPart, maxPartW - (maxPartSize * minNeighborSize));
			partitionsWeights.put(minCostNeighbor, minNeighborW + (maxPartSize * minNeighborSize));
			ArrayList<String> minNeighbors = directedGraph.get(minCostNeighbor);
			minNeighbors.add(maxCostPart);// should not be there
			directedGraph.put(minCostNeighbor, minNeighbors);
			ArrayList<String> maxNeighbours = directedGraph.get(maxCostPart);
			for (int k = 0; k < maxNeighbours.size(); k++)
				if (maxNeighbours.get(k).equalsIgnoreCase(minCostNeighbor))
					maxNeighbours.remove(k);
			directedGraph.put(maxCostPart, maxNeighbours);
			return true;
		} else {
			return false;
		}
	}

	/**
	 * @param maxCostPart: partition with maximum cost
	 * @return true if the maximum cost partition has a neighbor to flip
	 *         direction of arrow with, false if all neighbours investigated.
	 */
	public static boolean updateDirectedGraph(String maxCostPart) {
		ArrayList<String> flipNeighbors = directedGraph.get(maxCostPart);
		if ((flipNeighbors == null) || (flipNeighbors.size() == 0))
			return false;
		ArrayList<String> flipNeighborsClone = new ArrayList<String>(flipNeighbors.size());
		for (int i = 0; i < flipNeighbors.size(); i++)
			flipNeighborsClone.add(flipNeighbors.get(i));

		String minCostNeighbor, previousPart = null;
		while (true) {
			minCostNeighbor = getMinCostNeighbour(flipNeighborsClone);
			if (minCostNeighbor == null) {
				return false;
			}
			if (minCostNeighbor == previousPart)
				return false;
			previousPart = minCostNeighbor;
			if (inspectFlipping(maxCostPart, minCostNeighbor) && debugStages) {
				System.out.println("Stage2 -- Flipped " + maxCostPart + " to " + minCostNeighbor);
				printPartitionsStat("Stage2");
			}
			for (int i = 0; i < flipNeighbors.size(); i++)
				if (flipNeighborsClone.get(i).equalsIgnoreCase(minCostNeighbor)) {
					flipNeighborsClone.remove(i);
					break;
				}
		}
	}

	/**
	 * Applies stage 1 of load balancing to the partition with minimum weight.
	 * It directs all its adjacent partitions to it. It also removes the minimum
	 * partition from its neighbors list in the NG graph.
	 * @param minWeightPart
	 */
	public static void updateUndirectedGraph(String minWeightPart) {
		if (undirectedGraph.size() <= 1) {
			directedGraph.put(minWeightPart, new ArrayList<String>());
			undirectedGraph.clear();// directed graph does not have myself !
			return;
		}
		ArrayList<String> directedNeighborsToMinPart = new ArrayList<String>();
		ArrayList<String> neighboursToDirect = undirectedGraph.get(minWeightPart);
		for (int i = 0; i < neighboursToDirect.size(); i++) {
			String currentNeighbor = neighboursToDirect.get(i);
			directedNeighborsToMinPart.add(currentNeighbor);
			ArrayList<String> updatedNeighborList = undirectedGraph.get(currentNeighbor);
			for (int j = 0; j < updatedNeighborList.size(); j++) {
				if (updatedNeighborList.get(j).equalsIgnoreCase(minWeightPart)) {
					updatedNeighborList.remove(j);
					undirectedGraph.put(currentNeighbor, updatedNeighborList);
					long updatedMinW = partitionsWeights.get(currentNeighbor);
					updatedMinW = (updatedMinW - (partitionsSizes.get(currentNeighbor) * partitionsSizes
							.get(minWeightPart)));
					partitionsWeights.put(currentNeighbor, updatedMinW);
					break;
				}
			}
		}
		undirectedGraph.remove(minWeightPart);
		// calculatePartitionsWeight();//buggy
		directedGraph.put(minWeightPart, directedNeighborsToMinPart);
	}

	/**
	 * Stage 1
	 * @return partition's name that has minimum weight within the undirected
	 *         graph.
	 */
	public static String findMinWeight() {
		long minW = Long.MAX_VALUE;
		String part, minPart = null;
		Iterator<String> itr = undirectedGraph.keySet().iterator();
		while (itr.hasNext()) {
			part = itr.next();
			long w = partitionsWeights.get(part);
			if (w < minW) {
				minW = w;
				minPart = part;
			}
		}
		return minPart;
	}

	/**
	 * @param job Calculates each partition weight based on the undirected graph
	 *        for comparison. If static partitioning is applied then skipping
	 *        will be applied here.
	 */
	public static void initializePartitionsWeight() {
		ArrayList<String> neighbours;
		long pw = 0, me, other;
		for (int i = 0; i < partitionsNames.size(); i++) {
			String currentPartition = partitionsNames.get(i);
			me = partitionsSizes.get(currentPartition);
			neighbours = new ArrayList<String>();
			pw = me * me;
			for (int j = 0; j < partitionsNames.size(); j++) {
				other = partitionsSizes.get(partitionsNames.get(j));
				if (currentPartition.startsWith("G")) {
					String otherPartition = partitionsNames.get(j);
					int myRow = Reader.getRow(currentPartition);
					int myCol = Reader.getCol(currentPartition);
					int otherRow = Reader.getRow(otherPartition);
					int otherCol = Reader.getCol(otherPartition);
					if ((((myRow != myCol) && (myCol >= otherRow)) || ((otherRow != otherCol) && (otherCol >= myRow)))// new
							|| otherPartition.equals(currentPartition))
						continue;// verified

				}
				neighbours.add(partitionsNames.get(j));
				pw += me * other;
			}
			partitionsWeights.put(currentPartition, pw);
			undirectedGraph.put(currentPartition, neighbours);
		}
	}

	// public static void calculatePartitionsWeight() {
	// ArrayList<String> neighbours;
	// long pw = 0, me, other;
	// for (int i = 0; i < partitionsNames.size(); i++) {
	// String currentPartition = partitionsNames.get(i);
	// neighbours = undirectedGraph.get(currentPartition);
	// me = partitionsSizes.get(currentPartition);
	// pw = me * me;
	// if (neighbours != null) {
	// for (int j = 0; j < neighbours.size(); j++) {
	// other = partitionsSizes.get(neighbours.get(j));
	// pw += me * other;
	// }
	// }
	// partitionsWeights.put(currentPartition, pw);
	// }
	// }

	/**
	 * Calculates each partition cost (not including I/O now) over stage 1
	 * output of directed graph.
	 */
	// public static void calculatePartitionsCost() {
	// Iterator<String> itr = partitionsSizes.keySet().iterator();
	// while (itr.hasNext()) {
	// String currentPart = itr.next();
	// partitionsWeights.put(currentPart,
	// calculatePartitionCost(currentPart, partitionsSizes.get(currentPart)));
	// }
	// }

	// public static void printPartitionsWeight() {
	// ArrayList<String> neighbours;
	// long pw = 0, me, other;
	// for (int i = 0; i < partitionsNames.size(); i++) {
	// String currentPartition = partitionsNames.get(i);
	// System.out.println(currentPartition + " weight: "
	// + partitionsWeights.get(currentPartition));
	// }
	// // trend stage2
	// }

	public static long calculatePartitionCost(String currentPartition, long size) {
		long c = size * size;
		ArrayList<String> directedNeighbours = directedGraph.get(currentPartition);
		for (int j = 0; j < directedNeighbours.size(); j++)
			c += (size * partitionsSizes.get(directedNeighbours.get(j)));
		return c;
	}

	public static double calcWStandardDeviation(double avg) {
		double temp = 0;
		long w;
		for (int i = 0; i < partitionsNames.size(); i++) {
			w = partitionsWeights.get(partitionsNames.get(i));
			temp += (avg - w) * (avg - w);
		}
		return Math.sqrt(temp / partitionsNames.size());
	}

	public static void calcCWStandardDeviation() {// check
		Iterator<String> itr = partitionsCircularWeights.keySet().iterator();
		double avgWeight = 0, stdev = 0;
		long max = 0;
		for (int i = 0; i < partitionsCircularWeights.size(); i++) {
			long w = partitionsCircularWeights.get(itr.next());
			if (max < w)
				max = w;
			avgWeight += w;
		}
		avgWeight = avgWeight / partitionsNames.size();
		itr = partitionsCircularWeights.keySet().iterator();
		for (int i = 0; i < partitionsCircularWeights.size(); i++)
			stdev += (Math.pow((double) partitionsCircularWeights.get(itr.next()) - avgWeight, 2));
		System.out.println("Circular trend max:" + max + ", avg:" + avgWeight + ", stdev:"
				+ (Math.sqrt(stdev / partitionsSizes.size())));

		partitionsCircularWeights.clear();
		partitionsCircularWeights = null;
	}
}
