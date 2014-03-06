package edu.ucsb.cs.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.ucsb.cs.types.FeatureWeight;
import edu.ucsb.cs.types.FeatureWeightArrayWritable;

public class IdFeatureWeightArrayWritable extends FeatureWeightArrayWritable {

	public long id;

	public IdFeatureWeightArrayWritable() {
		super();
	}

	public IdFeatureWeightArrayWritable(int size) {
		super(size);
	}

	public IdFeatureWeightArrayWritable(long id, FeatureWeightArrayWritable element) {
		super(element.vectorSize, element.vector);
		this.id = id;
	}

	public IdFeatureWeightArrayWritable(long id, int size, FeatureWeight[] array) {
		super(size, array);
		this.id = id;
	}

	public void set(long id, FeatureWeightArrayWritable element) {
		this.id = id;
		this.vector = element.vector;
		this.vectorSize = element.vectorSize;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(id);
		super.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.id = in.readLong();
		super.readFields(in);
	}

	@Override
	public String toString() {
		return (id + " " + super.toString());
	}
}
