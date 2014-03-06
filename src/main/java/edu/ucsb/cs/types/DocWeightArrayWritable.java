package edu.ucsb.cs.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.ucsb.cs.types.DocWeight;

public class DocWeightArrayWritable implements Writable {

	public int vectorSize;
	public DocWeight[] vector;

	public void clear() {
		vectorSize = 0;
		vector = null;
	}

	public DocWeightArrayWritable() {}

	public DocWeightArrayWritable(int size) {
		this.vectorSize = size;
		this.vector = new DocWeight[size];
	}

	public DocWeightArrayWritable(int size, DocWeight[] array) throws IndexOutOfBoundsException {
		this.vectorSize = size;
		this.vector = array;
		if (this.vector.length != this.vectorSize)
			throw new IndexOutOfBoundsException();
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

	public float getMaxWeight() {
		float max = 0f;
		for (int i = 0; i < vectorSize; i++)
			if (max < vector[i].weight)
				max = vector[i].weight;
		return max;
	}

	public void addPair(long doc, float freq) {
		DocWeight[] newVector = new DocWeight[this.vectorSize + 1];
		for (int i = 0; i < this.vectorSize; i++)
			newVector[i] = new DocWeight(this.vector[i].getdocId(), this.vector[i].getWeigth());
		newVector[this.vectorSize] = new DocWeight(doc, freq);
		this.vector = newVector;
		this.vectorSize++;
	}

	public void CreateNewVector(int s) {
		this.vector = new DocWeight[s];
		for (int i = 0; i < s; i++)
			vector[i] = new DocWeight();
	}

	public void setVectorElement(int i, long feature, float freq) throws IndexOutOfBoundsException {
		if ((i >= this.vectorSize) || (i < 0))
			throw new IndexOutOfBoundsException();
		else
			this.vector[i] = new DocWeight(feature, freq);
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(this.vectorSize);
		for (int i = 0; i < this.vectorSize; i++) {
			out.writeLong(vector[i].getdocId());
			out.writeFloat(vector[i].getWeigth());
		}
	}

	public void readFields(DataInput in) throws IOException {
		this.vectorSize = in.readInt();
		this.CreateNewVector(this.vectorSize);
		for (int i = 0; i < this.vectorSize; i++) {
			this.vector[i].setLong(in.readLong());
			this.vector[i].setFloat(in.readFloat());
		}
	}

	public int getSize() {
		return this.vectorSize;
	}

	public DocWeight[] getVector() {
		return this.vector;
	}

	public long getDocID(int i) {
		return this.vector[i].docId;
	}

	public float getWeight(int i) {
		return this.vector[i].weight;
	}

}
