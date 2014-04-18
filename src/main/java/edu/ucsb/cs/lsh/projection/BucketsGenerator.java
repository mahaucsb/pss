package edu.ucsb.cs.lsh.projection;

import ivory.lsh.data.Permutation;
import ivory.lsh.data.PermutationByBit;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import edu.ucsb.cs.hybrid.Config;
import edu.ucsb.cs.lsh.types.BitSignature;
import edu.ucsb.cs.types.FeatureWeightArrayWritable;
import edu.ucsb.cs.utilities.JobSubmitter;
import edu.ucsb.cs.utilities.Properties;
import edu.umd.cloud9.io.SequenceFileUtils;
import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;

/**
 * This permute the signature vectors and collect those which have the same
 * signature within the same permutation number into one reducer to form a
 * bucket. See Ivory.GenerateChunkedPermutedTables which is slightly different
 * such that signatures from the same permutation are sorted then based on
 * window size B put into same bucket.
 */
public class BucketsGenerator {

	protected static String INPUT_DIR = SignaturesGenerator.OUTPUT_DIR;
	protected static String OUTPUT_DIR = "lshpartitions";

	public static void main(JobConf job) throws Exception {

		job.setJobName(BucketsGenerator.class.getSimpleName());
		Path inputPath = new Path(INPUT_DIR);
		Path outputPath = new Path(OUTPUT_DIR);

		FileSystem fs = FileSystem.get(job);
		if (fs.exists(outputPath))
			fs.delete(outputPath);

		job.setInputFormat(SequenceFileInputFormat.class);
		FileInputFormat.setInputPaths(job, inputPath);
		// Path[] paths = FileInputFormat.getInputPaths(job); //do I need this?
		FileOutputFormat.setOutputPath(job, outputPath);
		job.setOutputFormat(SequenceFileOutputFormat.class);

		job.set("mapred.child.java.opts", "-Xmx2048m");
		job.setInt("mapred.task.timeout", 600000000);
		// int numMappers = job.getInt("dummy", 2);
		int numReducers = job.getInt(ProjectionLshDriver.LSH_NPERMUTATION_PROPERTY,
				ProjectionLshDriver.LSH_NPERMUTATION_VALUE);

		job.setMapperClass(BucketMapper.class);
		job.setPartitionerClass(BucketPartitioner.class);
		// job.setNumMapTasks(numMappers);
		job.setNumReduceTasks(numReducers);
		job.setMapOutputKeyClass(PairOfIntSignature.class);
		job.setMapOutputValueClass(BitSignature.class);
		job.setReducerClass(BucketReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(FeatureWeightArrayWritable.class);

		// create Q permutation functions and write them to file
		int nBits = job.getInt(ProjectionLshDriver.LSH_NBITS_PROPERTY,
				ProjectionLshDriver.LSH_NBITS_VALUE);
		int numOfPermutations = job.getInt(ProjectionLshDriver.LSH_NPERMUTATION_PROPERTY,
				ProjectionLshDriver.LSH_NPERMUTATION_VALUE);
		String randomPermFile = "/randomPermutations";
		createPermutations(fs, job, randomPermFile, nBits, numOfPermutations);
		DistributedCache.addCacheFile(new URI(randomPermFile), job);
		setParameters(nBits, numOfPermutations);

		JobSubmitter.run(job,"LSH",-1);
	}

	public static void setParameters(int nBits, int numOfPermutations) {
		Properties.requiredParameters.add("  Number of projection vectors: " + nBits);
		Properties.requiredParameters.add("  Number of permutations: " + numOfPermutations);
	}

	/**
	 * Creates Q permutation functions and write them to file.
	 * @param numBits: number of random projections.
	 * @param numOfPermutations: number of permutation functions to write.
	 */
	public static void createPermutations(FileSystem fs, JobConf job, String randomPermFile,
			int numBits, int numOfPermutations) throws Exception {
		if (fs.exists(new Path(randomPermFile)))
			fs.delete(new Path(randomPermFile));

		SequenceFile.Writer writer = SequenceFile.createWriter(fs, job, new Path(randomPermFile),
				IntWritable.class, ArrayListOfIntsWritable.class);
		Permutation p = new PermutationByBit(numBits);
		for (int i = 0; i < numOfPermutations; i++) {
			ArrayListOfIntsWritable perm = p.nextPermutation();
			writer.append(new IntWritable(i), perm);
		}
		writer.close();
	}

	/**
	 * Produce permutations for each signature and map similar documents within
	 * same permutation to same bucket. <br>
	 * Ferhan:Maps each signature to Q random permutations, using the permuters
	 * stored in local cache file <docno,signature> -->
	 * <(permno,signature),docno>
	 */

	public static class BucketMapper extends MapReduceBase implements
			Mapper<LongWritable, BitSignature, PairOfIntSignature, BitSignature> {

		static Path[] localFiles;
		static List<Writable> randomPermutations;
		static int numOfPermutations;
		static BitSignature permutedSignature;
		private PairOfIntSignature pairKey = new PairOfIntSignature();

		@Override
		public void configure(JobConf job) {
			numOfPermutations = job.getInt(ProjectionLshDriver.LSH_NPERMUTATION_PROPERTY,
					ProjectionLshDriver.LSH_NPERMUTATION_VALUE);
			int numOfBits = job.getInt(ProjectionLshDriver.LSH_NBITS_PROPERTY,
					ProjectionLshDriver.LSH_NBITS_VALUE);

			permutedSignature = new BitSignature(numOfBits);
			try {
				localFiles = DistributedCache.getLocalCacheFiles(job);
				randomPermutations = SequenceFileUtils.readValues(localFiles[0],
						FileSystem.getLocal(job));
			} catch (IOException e) {
				throw new RuntimeException("ERROR: cannot read random permutations ..\n");
			}

		}

		public void map(LongWritable docno, BitSignature signature,
				OutputCollector<PairOfIntSignature, BitSignature> output, Reporter reporter)
				throws IOException {

			for (int i = 0; i < numOfPermutations; i++) {
				signature.perm((ArrayListOfIntsWritable) randomPermutations.get(i),
						permutedSignature);
				permutedSignature.setVector(signature.vector);
				try {
					pairKey.setInt(i);
					pairKey.setSignature(permutedSignature);
				} catch (Exception e) {
					throw new RuntimeException("pairKey type:" + pairKey.getClass().toString()
							+ ",permuatedSig type:" + permutedSignature.getClass().toString()
							+ "output.collect exception: \n" + e.toString());
				}
				output.collect(pairKey, signature);
			}
		}
	}

	public static class BucketPartitioner implements Partitioner<PairOfIntSignature, BitSignature> {
		public void configure(JobConf conf) {}

		public int getPartition(PairOfIntSignature key, BitSignature value, int numReducers) {
			return key.getInt() % numReducers;
		}
	}

	// all signatures:
	// (1) belong to the same permutation table, and
	// (2) sorted
	// overlapSize = conf.getInt("Ivory.OverlapSize", -1);
	// chunckSize = conf.getInt("Ivory.ChunckSize", -1);

	public static class BucketReducer extends MapReduceBase implements
			Reducer<PairOfIntSignature, BitSignature, LongWritable, FeatureWeightArrayWritable> {
		String lastSig = null;
		LongWritable keyId = new LongWritable();
		FeatureWeightArrayWritable emptyVal = new FeatureWeightArrayWritable(0);

		// int overlapCount;

		// @Override
		// public void configure(JobConf job) {
		// float overlapThreshold =
		// job.getFloat(ProjectionLshDriver.LSH_OVERLAP_PROPERTY,
		// ProjectionLshDriver.LSH_OVERLAP_VALUE);
		// int numProjections =
		// job.getInt(ProjectionLshDriver.LSH_NBITS_PROPERTY,
		// ProjectionLshDriver.LSH_NBITS_VALUE);
		// overlapCount = (int) (numProjections * overlapThreshold);
		// }

		/**
		 * @param key: same permutation number with signatures arriving sorted.<br>
		 * @param value: (doc,docno,signature) where signatures and permNo is
		 *        the same per key.
		 */
		public void reduce(PairOfIntSignature key, Iterator<BitSignature> val,
				OutputCollector<LongWritable, FeatureWeightArrayWritable> output, Reporter reporter)
				throws IOException {
			while (val.hasNext()) {
				BitSignature nexVal = val.next();

				// if (CountOverlap(leadSig, nexVal) < overlapCount) {
				// if ((lastSig != null) && (CountOverlap(leadSig, nexVal) <
				// overlapCount)) {
				if ((lastSig != null) && (nexVal.toString().split(" ")[0].equals(lastSig))) {
					keyId.set(nexVal.docno);
					output.collect(keyId, nexVal.vector);
				} else {
					keyId.set(0);
					output.collect(keyId, emptyVal);
					keyId.set(nexVal.docno);
					output.collect(keyId, nexVal.vector);
				}
				lastSig = nexVal.toString().split(" ")[0];
			}
		}
	}

	public static int CountOverlap(String s1, BitSignature sig2) {
		String s2 = sig2.toString().split(" ")[0];
		int overlapCount = 0;
		for (int i = 0; i < s1.length() && i < s2.length(); i++)
			if (s1.charAt(i) == s2.charAt(i))
				overlapCount++;
		return overlapCount;
	}
	// public static void addDistribuedCache(JobConf job, String srcPath) {
	// try {
	// DistributedCache.addFileToClassPath(new Path("seqvectors1/part-00002"),
	// job);
	// } catch (IOException e) {
	// e.printStackTrace();
	// }
	// }
}