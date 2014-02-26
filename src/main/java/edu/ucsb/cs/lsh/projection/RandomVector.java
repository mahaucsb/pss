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
 * @Since Dec 27, 2012
 */

package edu.ucsb.cs.lsh.projection;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * A data structure to optimize space when storing an array of floats. Floats
 * are quantized into byte values and stored as a BytesWritable. maxNorm is a
 * maxNormalization factor required to re-compute float from byte.
 * 
 * byte = quantize(float) = ((float/maxNorm)*128)==128 ? 127 : ((float/maxNorm)
 * float = dequantize(byte) = ((byte/128)*maxNorm)
 * 
 * @author Maha
 * 
 */
public class RandomVector implements Writable {
	byte[] bytes;
	float maxNorm, minNorm;

	public RandomVector() {
		super();
	}

	@Override
	public String toString() {
		return bytes.length + "";
	}

	public RandomVector(byte[] bytearray, float max, float min) {
		super();
		bytes = new byte[bytearray.length];
		for (int i = 0; i < bytearray.length; i++) {
			bytes[i] = bytearray[i];
		}
		maxNorm = max;
		minNorm = min;
	}

	public void readFields(DataInput in) throws IOException {
		maxNorm = in.readFloat();
		minNorm = in.readFloat();
		bytes = new byte[in.readInt()];
		for (int i = 0; i < bytes.length; i++) {
			bytes[i] = in.readByte();
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeFloat(maxNorm);
		out.writeFloat(minNorm);
		out.writeInt(bytes.length);
		for (int i = 0; i < bytes.length; i++) {
			out.writeByte(bytes[i]);
		}
	}

	public byte get(int index) {
		return bytes[index];
	}

	public float getAsFloat(int index) {
		byte f1 = get(index);
		if (f1 > 0) {
			return (f1 * maxNorm / 128f);
		} else {
			return (-f1 * minNorm / 128f);
		}
	}

	public int size() {
		return bytes.length;
	}
}
