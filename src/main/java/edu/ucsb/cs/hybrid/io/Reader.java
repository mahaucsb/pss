package edu.ucsb.cs.hybrid.io;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

import edu.ucsb.cs.bruteforce.ForwardMapper;
import edu.ucsb.cs.hybrid.Config;
import edu.ucsb.cs.partitioning.jaccard.JaccardCoarsePartitionMain;
import edu.ucsb.cs.types.FeatureWeightArrayWritable;
import edu.ucsb.cs.types.IdFeatureWeightArrayWritable;
import edu.ucsb.cs.types.TextArrayWritable;

/**
 * Note:Sept 17th 2013, disabled baraglia vector stuff.
 * <p>
 * Opens other files from input to compare with this map assigned partition .<br>
 * Due to the symmetry of the similarity computation, this class uses a
 * "circular load balancing" if desired such that each partition compares with
 * another and not the vice versa.This is implemented using the following
 * criteria:
 * 
 * <pre>
 * Assume m mappers, c combinations = (m(m-1)/2) , with mapper id's = 0,1,2...,
 * files named 0,1,2... . Then:<br>
 * map-i opens files i,i+1,...,(i+(c/m)+1)%m if i < (c%m) <br>
 * Else <br>
 * map-i opens files i,i+1,...,(i+(c/m))%m if i > (c%m)
 * </pre>
 * 
 * The class also gives the option of "static partitioning", that is to skip
 * partitions that are definitely dissimilar to the one hosted by this map task.
 * The criteria used to implement this rule is through partitions naming:
 * 
 * <pre>
 * ...
 * </pre>
 * 
 * Furthermore, it performs the dot product between the global vectors. Each
 * vector represents the max weighted vector of its own partition. This is to
 * help filter our dissimilar partitions before fetching a partition for
 * comparison. Finally, if desired then each fetched vector is dot product with
 * this partition dummmy vector to check for possible candidates availability.
 */
/*
 * circular isn't working right !
 */
public class Reader {

	public int nFiles, nBVectors = 0, nbVectors, ioBlockSize, ioBlockStart = 0;
	FileSystem hdfs = null;
	JobConf conf;
	public FileStatus[] cachedFiles;
	public boolean readMyPartition;
	public String myPath;
	public SequenceFile.Reader reader = null;
	public IdFeatureWeightArrayWritable[] ioBlock = null, compBlock = null;
	public int i, myRow, myCol;
	public boolean staticPart, circular, partitionsDot, partVecDot, excludeMyself;
	public java.util.TreeSet<Integer> circularFiles;
	public MapFile.Reader jaccardFiles=null;//new
	public java.util.TreeSet<String> loadbalanceFiles;
	public FeatureWeightArrayWritable myBaragliaVector;
	public float threshold;
	public int loadbalance;
	public String metric;

	public Reader(JobConf job, Path inputPath, int blockSize) throws IOException {
		conf = job;
		metric=job.get(Config.METRIC_PROPERTY, Config.METRIC_VALUE);
		staticPart = job
				.getBoolean(Config.STATIC_PARTITION_PROPERTY, Config.STATIC_PARTITION_VALUE);
		circular = job.getBoolean(Config.CIRCULAR_PROPERTY, Config.CIRCULAR_VALUE);
		partitionsDot = job.getBoolean(Config.PARTITIONS_DOT_PROPERTY, Config.PARTITIONS_DOT_VALUE);
		partVecDot = job.getBoolean(Config.PART_DOT_VECTOR_PROPERTY, Config.PART_DOT_VECTOR_VALUE);
		excludeMyself = job.getBoolean(Config.EXCLUDE_MYSELF_PROPERTY, Config.EXCLUDE_MYSELF_VALUE);
		loadbalance = job.getInt(Config.LOAD_BALANCE_PROPERTY, Config.LOAD_BALANCE_VALUE);// not fully implemneted
		ioBlockSize = job.getInt(Config.BLOCK_SIZE_PROPERTY, Config.BLOCK_SIZE_VALUE);
		ioBlock = new IdFeatureWeightArrayWritable[ioBlockSize];
		compBlock = new IdFeatureWeightArrayWritable[blockSize];
		getFiles(job, inputPath.getParent()); // this should be circular's else
		myPath = inputPath.getName();
		distributePartitions(job, inputPath); //setup
	}

	/**
	 * Set up configurations to help find the other partitions to compare with,
	 * for each mapper task based on static partitioning check, load balancing
	 * and other configurations if they are set (SIGIR'14).
	 * @param job: to be run and contains configurations.
	 * @param inputPath: input directory of this job.
	 * @throws IOException
	 */
	public void distributePartitions(JobConf job, Path inputPath) throws IOException {
		if (loadbalance != 0) {
			loadbalanceFiles = new java.util.TreeSet<String>();
			MapFile.Reader mapReader = new MapFile.Reader(hdfs,
					TwoStageLoadbalancing.partitionComparisonList.getName(), conf);
			TextArrayWritable neighbors = new TextArrayWritable();
			mapReader.get(new Text(inputPath.getName()), neighbors);
			if (neighbors != null) {
				for (int i = 0; i < neighbors.size(); i++) {
					loadbalanceFiles.add(neighbors.get(i));// add others
				}
			}
			loadbalanceFiles.add(myPath);
		} else {
			if(metric.toLowerCase().contains("jac")){
				 String inDir = JaccardCoarsePartitionMain.JACCARD_SKIP_PARTITIONS;
				 getJaccardFiles(job, new Path(inDir));
				 System.out.println("check: my path is "+myPath); //remove
				 IntWritable partitionKey = new IntWritable(Integer.parseInt(myPath));
				 Text skipListVal = new Text();
				 jaccardFiles.get(partitionKey, skipListVal);
				 if(skipListVal!= null)
					 System.out.println("check: yaaay "+skipListVal.toString());//remove
				 else
					 System.out.println("check: boooo"); //remove
			}else if ((staticPart)&&(metric.toLowerCase().contains("cos"))) {
				myRow = getRow(inputPath.getName());
				myCol = getCol(inputPath.getName());
			}
			if (circular) { 
				//Doesn't take into consideration static skipping
				int mapId = 0, start = 0;
				for (i = 0; i < cachedFiles.length; i++)
					if (cachedFiles[i].getPath().getName().equalsIgnoreCase(inputPath.getName())) {
						mapId = i;
						break;
					}
				circularFiles = new java.util.TreeSet<Integer>();
				int combin = (nFiles * (nFiles - 1) / 2);
				if (excludeMyself)
					start = 1;
				for (i = mapId + start; i <= mapId + (combin / nFiles); i++) {
					circularFiles.add(i % nFiles);
				}
				if (mapId < (combin % nFiles)) {
					circularFiles.add((mapId + (combin / nFiles) + 1) % nFiles);
				}
			}
			if ((partitionsDot) || partVecDot) {
				// MapFile.Reader mapReader = new MapFile.Reader(hdfs,
				// Collector.baragliaPath.getName(),
				// conf);
				// myBaragliaVector = new FeatureWeightArrayWritable();
				// mapReader
				// .get(new Text("G" + getRow(inputPath.getName()) + "_"
				// + getCol(inputPath.getName())), myBaragliaVector);
				// threshold = job.getFloat(Config.THRESHOLD_PROPERTY,
				// Config.THRESHOLD_VALUE);
			}
		}
	}

	/**
	 * Prepares global variables to point at the files/partitions to compare
	 * with.
	 * @param job
	 * @param inputPath : path where all input files exists.
	 */
	public void getFiles(JobConf job, Path inputPath) {
		try {
			hdfs = FileSystem.get(job);
			if (!hdfs.exists(inputPath)) {
				throw new UnsupportedEncodingException("InputPath doesn't exists in hdfs !");
			}
			cachedFiles = hdfs.listStatus(inputPath);
			nFiles = cachedFiles.length;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
//new
	public void getJaccardFiles(JobConf job, Path inputPath) {
		try {
			hdfs = FileSystem.get(job);
			if (!hdfs.exists(inputPath)) {
				throw new UnsupportedEncodingException(inputPath.toString()+" doesn't exists in hdfs !");
			}
			jaccardFiles = new MapFile.Reader(hdfs,inputPath.getName(), job);
			System.out.println("check: reading jaccard files");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Prepares a reader for the given file number and checks if this
	 * partition/file passes the skipping techniques.
	 */
	public boolean setReader(int fileNo) throws IOException {

		Path otherPath = cachedFiles[fileNo].getPath();
		if (hdfs.isDirectory(otherPath))
			return false;
		if (otherPath.getName().equalsIgnoreCase(myPath))
			if (excludeMyself)
				return false;
			else
				readMyPartition = true;
		else
			readMyPartition = false; 
		if (loadbalance != 0) //check:this should be combind with preparePartitions!
			if (loadbalanceFiles.contains(otherPath.getName())) {
				reader = new SequenceFile.Reader(hdfs, otherPath, conf);
				return true;
			} 
			else
				return false;
		if (staticPart){
			if(metric.equalsIgnoreCase("cosine")){
				if(CosineSkipPartition(otherPath.getName())) 
					return false;
			}else{
				if(JaccardSkipPartition(otherPath.getName()))
					return false;
			}
		}
		if ((circular) && (!circularFiles.contains(fileNo))) {
			return false;
		}
		if ((partitionsDot)
				&& (satisfyPartitionDotProduct(cachedFiles[fileNo].getPath().getName()))) {
			return false;
		}
		reader = new SequenceFile.Reader(hdfs, otherPath, conf);
		return true;
	}

	/**
	 * @param otherFile
	 * @return True if the otherFile partition name is skipped by this map
	 *         partition according to the Cosine static partitioning naming system.
	 */
	public boolean CosineSkipPartition(String otherFile) {
		int otherRow = getRow(otherFile);
		int otherCol = getCol(otherFile);
		if (((myRow != otherRow) || (myCol != otherCol))
				&& ((otherCol != otherRow) && (otherCol >= myRow))
				|| ((myCol != myRow) && ((myCol >= otherRow))))
			return true;
		return false;
	}


	/**
	 * @param otherFile
	 * @return True if the otherFile partition name is skipped by this map
	 *         partition according to the Jaccard static partitioning.
	 * @throws IOException 
	 */
	public boolean JaccardSkipPartition(String otherFile) throws IOException {
		MapFile.Reader mapReader = new MapFile.Reader(hdfs,
				JaccardCoarsePartitionMain.JACCARD_SKIP_PARTITIONS,
				conf);
		Text skipList = new Text();
		mapReader.get(new IntWritable(Integer.parseInt(myPath.substring(0,myPath.indexOf('-')))),skipList);
		StringTokenizer stk = new StringTokenizer(skipList.toString());
		while(stk.hasMoreTokens())
			if(stk.nextToken().contains(otherFile.substring(0,myPath.indexOf('-'))))
				return true;	
		return false;
	}

	LongWritable key = new LongWritable();
	FeatureWeightArrayWritable value = new FeatureWeightArrayWritable();

	/**
	 * @param otherFile
	 * @return True if the dot product between the otherFile's partitions
	 *         globalVector and this.map partition globalVecor is less than
	 *         threshold, otherwise false.
	 * @throws IOException
	 */
	public boolean satisfyPartitionDotProduct(String otherFile) throws IOException {
		// MapFile.Reader mapReader = new MapFile.Reader(hdfs,
		// Collector.baragliaPath.getName(), conf);
		// mapReader.get(new Text("G" + getRow(otherFile) + "_" +
		// getCol(otherFile)), value);
		// return (ForwardMapper.dotLessThan(myBaragliaVector, value,
		// threshold));
		return true;
	}

	public boolean satisfyVectorPartitionDotProduct(FeatureWeightArrayWritable vector)
			throws IOException {
		return (ForwardMapper.dotLessThan(myBaragliaVector, vector, threshold));
	}

	/**
	 * Fetches block of "ioBlockSize" vectors after checking the possibility of
	 * candidacy with this partition.
	 * @param ioBlockSize: number of vectors to be read at once from others.
	 * @return a block of vectors.
	 */
	public int fetchNextioBlock() {
		try {
			ioBlockStart = 0;
			nBVectors = 0;
			for (i = 0; i < ioBlockSize; i++) {
				if (!reader.next(key, value)) {
					reader.close();
					reader = null;
					nBVectors = i;
					return nBVectors;
				} else {
					if (partVecDot && satisfyVectorPartitionDotProduct(value)) {
						i--;
					} else {
						ioBlock[i] = null;
						ioBlock[i] = new IdFeatureWeightArrayWritable(key.get(), value.vectorSize,
								value.vector);
					}
				}
			}
			nBVectors = ioBlockSize;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			return nBVectors;
		}
	}

	/**
	 * returns a block of "blockSize" vectors from the ioBlock fetched.
	 * @param blockSize: number of vectors to be processed at once from others.
	 * @return a block of vectors.
	 */
	public IdFeatureWeightArrayWritable[] getNextbVectors(int blockSize) {
		nbVectors = 0;
		if ((ioBlockStart >= nBVectors) && (fetchNextioBlock() == 0))
			return null;
		for (i = ioBlockStart; i < (ioBlockStart + blockSize) && i < nBVectors; i++, nbVectors++)
			compBlock[nbVectors] = ioBlock[i];
		ioBlockStart += nbVectors;
		return compBlock;
	}

	public static int getCol(String fileName) {
		if (fileName.contains("-"))
			return (Integer.parseInt(fileName.substring(fileName.indexOf("_") + 1,
					fileName.indexOf('-'))));
		else
			return (Integer.parseInt(fileName.substring(fileName.indexOf("_") + 1)));
	}

	public static int getRow(String fileName) {
		try {
			return Integer.parseInt(fileName.substring(1, fileName.indexOf("_")));
		} catch (Exception e) {
			System.out.println("Error: Files are not named Gi_j !! Plsease set hybrid.static.partition=false in conf/hybrid/conf.xml ");
			System.exit(0);
		}
		return 0;
	}
}
