package edu.ucsb.cs.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Writable;

import edu.ucsb.cs.types.FeatureWeight;

public class FeatureWeightArrayWritable implements Writable {

	public int vectorSize;
	public FeatureWeight[] vector;

	public FeatureWeightArrayWritable() {}

	public FeatureWeightArrayWritable(int size) {
		this.vectorSize = size;
		this.vector = new FeatureWeight[size];
	}

	public FeatureWeightArrayWritable(int size, FeatureWeight[] array)
			throws IndexOutOfBoundsException {
		this.vectorSize = size;
		this.vector = array;
		if (this.vector.length != this.vectorSize)
			throw new IndexOutOfBoundsException();
	}

	// Sort increasingly 1,2... where 1=most popular
	public FeatureWeight[] sortById() {
		if (!(vector == null || vector.length == 0))
			Arrays.sort(vector);
		return vector;
	}

	public FeatureWeight[] sortByWeight() {
		if (vector == null || vector.length == 0)
			return null;

		Comparator<FeatureWeight> weightComparator = new Comparator<FeatureWeight>() {
			public int compare(FeatureWeight o1, FeatureWeight o2) {
				return (int) (o1.weight - o2.weight);
			}
		};
		Arrays.sort(vector, weightComparator);
		return vector;
	}

	public class DecreaseWeight implements Comparator<FeatureWeight> {

		public int compare(FeatureWeight o1, FeatureWeight o2) {
			return (o1.weight > o2.weight ? -1 : 1);
		}
	}

	public void clear() {
		this.vector = null;
		this.vectorSize = 0;
	}

	public FeatureWeightArrayWritable(String words) {
		HashMap<String, Long> wordFreq = new HashMap<String, Long>();
		StringTokenizer tkz = new StringTokenizer(words, ", ");
		while (tkz.hasMoreTokens()) {
			String word = tkz.nextToken();
			if (wordFreq.containsKey(word))
				wordFreq.put(word, wordFreq.get(word) + 1);
			else
				wordFreq.put(word, (long) 1);
		}
		Iterator<String> features = wordFreq.keySet().iterator();
		float sqrtsum = 0f;
		while (features.hasNext()) {
			float f = wordFreq.get(features.next());
			sqrtsum += (f * f);
		}
		sqrtsum = (float) Math.sqrt(sqrtsum);

		features = wordFreq.keySet().iterator();
		this.vector = new FeatureWeight[wordFreq.size()];
		int i = 0;
		while (features.hasNext()) {
			String feat = features.next();
			vector[i++] = new FeatureWeight(i, wordFreq.get(feat) / sqrtsum);
		}
		this.vectorSize = vector.length;
	}

	public void set(int size, FeatureWeight[] array) {
		this.vectorSize = size;
		this.vector = array;
	}

	public void set(int size) {
		this.vectorSize = size;
		this.vector = null;
		System.gc();
		this.vector = new FeatureWeight[size];
		for (int i = 0; i < this.vectorSize; i++)
			this.vector[i] = new FeatureWeight();
	}

	@Override
	public String toString() {
		StringBuilder buffer = new StringBuilder();
		for (int i = 0; i < this.vectorSize; i++)
			buffer.append(vector[i].toString() + " ");
		return buffer.toString();
	}

	public float getNorm1() {
		float norm = 0f;
		for (int i = 0; i < vectorSize; i++)
			norm += vector[i].weight;
		return norm;
	}

	public float getPNorm(float p) {
		if (p == 0)
			return getMaxWeight();
		else if (p == 1)
			return getNorm1();
		else {
			double norm = 0f;
			for (int i = 0; i < vectorSize; i++)
				norm += Math.pow(vector[i].weight, p);
			return (float) Math.pow(norm, 1 / p);
		}
	}

	public float getPNormPowered(float p) {
		if (p == 0)
			return getMaxWeight();
		else if (p == 1)
			return getNorm1();
		else {
			double norm = 0f;
			for (int i = 0; i < vectorSize; i++)
				norm += Math.pow(vector[i].weight, p);
			return (float) norm;
		}
	}

	public float getQHolderInquality(float p) {
		if (p == 1)
			return 0;
		else if (p == 0)
			return 1;
		else
			return (1 / (1 - 1 / p));
	}

	public float getQNorm(float p) {
		return getPNorm(getQHolderInquality(p));
	}

	public float getQNormPowered(float p) {
		return getPNormPowered(getQHolderInquality(p));
	}

	public float getMaxWeight() {
		float max = 0f;
		for (int i = 0; i < vectorSize; i++)
			if (max < vector[i].weight)
				max = vector[i].weight;
		return max;
	}

	public void addPair(long f, float w) {
		FeatureWeight[] newVector = new FeatureWeight[this.vectorSize + 1];
		for (int i = 0; i < this.vectorSize; i++)
			newVector[i] = new FeatureWeight(this.vector[i].getFeature(),
					this.vector[i].getWeigth());
		newVector[this.vectorSize] = new FeatureWeight(f, w);
		this.vector = newVector;
		this.vectorSize++;
	}

	public void CreateNewVector(int s) {
		this.vector = new FeatureWeight[s];
		for (int i = 0; i < s; i++)
			vector[i] = new FeatureWeight();
	}

	public void setElement(int i, long feature, float w) {
		this.vector[i].feature = feature;
		this.vector[i].weight = w;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(this.vectorSize);
		for (int i = 0; i < this.vectorSize; i++) {
			out.writeLong(vector[i].getFeature());
			out.writeFloat(vector[i].getWeigth());
		}
	}

	public void readFields(DataInput in) throws IOException {
		this.vectorSize = in.readInt();
		this.CreateNewVector(this.vectorSize);
		for (int i = 0; i < this.vectorSize; i++) {
			this.vector[i].setFeature(in.readLong());
			this.vector[i].setWeight(in.readFloat());
		}
	}

	public int getSize() {
		return this.vectorSize;
	}

	public FeatureWeight[] getVector() {
		return this.vector;
	}

	public long getFeature(int i) {
		return this.vector[i].feature;
	}

	public float getWeight(int i) {
		return this.vector[i].weight;
	}

}
