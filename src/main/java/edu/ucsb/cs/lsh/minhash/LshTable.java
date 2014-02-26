package edu.ucsb.cs.lsh.minhash;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;

import org.apache.hadoop.io.Writable;

import edu.ucsb.cs.lsh.types.CounterWritable;

/**
 * This class is responsible for producing "l" LSH signatures for each document
 * and saving the results in buckets. The documents colliding in the same bucket
 * need to be compared with each other. In the code a bucket is a "MinHashTable"
 * where each table saves up to (n.k) buckets where n is the total number of
 * documents and k is the number of concatenated hash values that forms a
 * signature.
 * 
 * @author Maha
 */
public class LshTable implements Writable {

	// buckets is inside those 'l' minhashtables
	private int l, m;
	private MinhashTable[] minhashTables;
	public int k, P; // initialized which new LSHTable
	private int[] a, b; // initialized which new LSHTable
	private float threshold;

	public LshTable() {}

	public LshTable(int k1, int l1, int m1, long d, float t) { // (5)
		this.l = l1;
		this.m = m1;
		this.k = k1; // number of minhashes to be concatenated
		this.threshold = t;
		// Initialize array of m random linear projections
		a = new int[m];
		b = new int[m];
		for (int i = 0; i < m; i++) {
			a[i] = 1 + (int) Math.floor(Math.random() * (d - 1));
			b[i] = (int) Math.floor(Math.random() * d);
		}
		P = (int) getPrime(d);

		// Array of l minhash tables
		minhashTables = new MinhashTable[l];
		for (int i = 0; i < l; i++)
			minhashTables[i] = new MinhashTable(k, m, i);
	}

	private class MinhashTable implements Writable {

		private int idx; // table id
		private int k;
		private int[] indices; // permutation indices

		public MinhashTable() {}

		// Draw k random permutations for each table
		private MinhashTable(int k1, int m, int idx1) {
			this.idx = idx1;
			this.k = k1;
			this.indices = new int[k];
			for (int i = 0; i < k; i++)
				indices[i] = (int) Math.floor(Math.random() * m);
		}

		// Minhash Signature extraction
		private int[] getSignature(CounterWritable counter) {
			int[] signature = new int[k];
			long q;
			long[] keys = counter.keySet();// page features
			// For each hashValue pick minHash of page features
			for (int i = 0; i < k; i++) {
				signature[i] = Integer.MAX_VALUE;
				for (int j = 0; j < keys.length; j++) {
					q = ((a[indices[i]] * keys[j]) + b[indices[i]]) % P;
					if (q < signature[i])
						signature[i] = (int) q; // sig[i]=minhash of page
					// features
				}
			}
			return signature;
		}

		public void readFields(DataInput in) throws IOException {
			this.idx = in.readInt();
			this.k = in.readInt();
			this.indices = new int[k];
			for (int i = 0; i < k; i++)
				this.indices[i] = in.readInt();
		}

		public void write(DataOutput out) throws IOException {
			out.writeInt(idx);
			out.writeInt(k);
			for (int i = 0; i < this.k; i++)
				out.writeInt(indices[i]);
		}
	}

	public ArrayList<int[]> getSignatures(CounterWritable counter) {
		ArrayList<int[]> sigs = new ArrayList<int[]>();
		for (int i = 0; i < minhashTables.length; i++)
			sigs.add(minhashTables[i].getSignature(counter));
		return sigs;
	}

	// done at reducers
	public void deduplicateIndex(LinkedHashSet<CounterWritable> bucket) {
		int x = 0;
		// not needed
		HashSet<CounterWritable> union = new HashSet<CounterWritable>();
		Iterator<CounterWritable> itr = bucket.iterator();
		while (itr.hasNext())
			union.add(itr.next());

		// Check for near duplicates
		for (CounterWritable CounterWritable1 : union) {
			for (CounterWritable CounterWritable2 : union) {
				double sim = 1.0;
				if (CounterWritable1 != CounterWritable2
						&& (CounterWritable1.SHA1.equals(CounterWritable2.SHA1) || (sim = getJaccard(
								CounterWritable1, CounterWritable2, threshold)) >= threshold)) {
					// CounterWritable1 and CounterWritable2 are duplicates
					// System.out.println(CounterWritable.docid + "\t" +
					// CounterWritable2.docid
					// +
					// "\t"
				}
			}
		}
	}

	// Jaccard similarity generalized for multi-sets (weighted dimensions)
	public static double getJaccard(CounterWritable index1, CounterWritable index2, double threshold) {
		double min, max, s_min = 0, s_max = 0, bound = 0;
		double upper_max = Math.max(index1.totalCount, index2.totalCount);
		double upper_union = index1.totalCount + index2.totalCount;
		int c1, c2, s_c1 = 0, s_c2 = 0;

		for (long key : index1.keySet()) {
			c1 = index1.getCount(key);
			c2 = index2.getCount(key);
			min = Math.min(c1, c2);
			max = Math.max(c1, c2);
			s_min += min;
			s_max += max;
			s_c1 += c1;
			s_c2 += c2;

			// Early threshold break for pairwise CounterWritable comparison
			bound += max - min;
			if ((upper_max - bound) / upper_max < threshold)
				return 0;
			else if (s_min / upper_union >= threshold)
				return 1;
		}

		return s_min / (s_max + (index1.totalCount - s_c1) + (index2.totalCount - s_c2));
	}

	private static long getPrime(long n) {
		while (!isPrime(n))
			n++;
		return n;
	}

	private static boolean isPrime(long n) {
		if (n <= 2)
			return n == 2;
		else if (n % 2 == 0)
			return false;
		for (int i = 3, end = (int) Math.sqrt(n); i <= end; i += 2)
			if (n % i == 0)
				return false;
		return true;
	}

	public void readFields(DataInput in) throws IOException {
		this.l = in.readInt();
		this.m = in.readInt();
		this.minhashTables = new MinhashTable[l];
		for (int i = 0; i < this.l; i++) {
			this.minhashTables[i] = new MinhashTable();
			this.minhashTables[i].readFields(in);
		}
		k = in.readInt();
		P = in.readInt();
		this.a = new int[m];
		for (int i = 0; i < m; i++)
			a[i] = in.readInt();
		this.b = new int[m];
		for (int i = 0; i < m; i++)
			b[i] = in.readInt();
		this.threshold = in.readFloat();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(l);
		out.writeInt(m);
		for (int i = 0; i < this.l; i++)
			this.minhashTables[i].write(out);
		out.writeInt(k);
		out.writeInt(P);
		for (int i = 0; i < m; i++)
			out.writeInt(a[i]);
		for (int i = 0; i < m; i++)
			out.writeInt(b[i]);
		out.writeFloat(threshold);
	}
}
