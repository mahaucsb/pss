package edu.ucsb.cs.preprocessing.cleaning;

/**
 * The class below takes as input the full path to the gzipped WARC files directory on the command line,
 *  iterates through these files, and prints out each TREC ID in the WARC file as well as the Target 
 *  URI for that TREC ID.<br>
 *  Output is a bag of cleaned words.
 */
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

public class WarcFileCleaner {

	public static void printUsage() {
		System.out.println("Usage: <inputDir of warc.gz files> <outputDir of cleaned files>");
	}

	public static void main(String[] args) throws IOException {

		if (args.length == 2) {
			String inputdir = args[0];
			String outputDir = args[1];
			if (DirExists(inputdir)) {
				if (DirExists(outputDir))
					deleteDir(new File(outputDir));
				(new File(args[1])).mkdir();
				recursiveTraverse(new File(inputdir), outputDir);
			} else
				System.out.println("Input path for directory doesn't exist in local FS!");
		} else
			printUsage();
	}

	public static void recursiveTraverse(File dir, String outputDir) {
		if (dir.isDirectory())
			for (File file : dir.listFiles())
				recursiveTraverse(file, outputDir);
		else if (dir.getName().contains(".warc.gz")) {
			cleanWarcFiles(dir, outputDir); // Eg. "/Users/Hadoop/00.warc.gz"
		}
	}

	static long outFileNo = 0;

	/**
	 * Unzips Warc folders and cleans body from wierd characters and html 
	 * symbols and output text file with ID : <bag of words>
	 * @param inputWarcFile
	 * @param outDir
	 */
	public static void cleanWarcFiles(File inputWarcFile, String outDir) {
		BufferedWriter outFile = createFile(outDir + "/" + (++outFileNo));
		// open our gzip input stream
		GZIPInputStream gzInputStream = null;
		try {
			gzInputStream = new GZIPInputStream(new FileInputStream(inputWarcFile.getPath()));
			// cast to a data input stream
			DataInputStream inStream = new DataInputStream(gzInputStream);

			WarcRecord thisWarcRecord;
			while ((thisWarcRecord = WarcRecord.readNextWarcRecord(inStream)) != null) {
				// see if it's a response record
				if (thisWarcRecord.getHeaderRecordType().equals("response")) {
					// it is - create a WarcHTML record
					WarcHTMLResponseRecord htmlRecord = new WarcHTMLResponseRecord(thisWarcRecord);
					String id= htmlRecord.getTargetTrecID();
					String content = htmlRecord.getRawRecord().getContentUTF8();
					outFile.write(id+" : "+PageCleaner.startProcess(content) + "\n");
				}
			}
			inStream.close();
			outFile.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// //////////////////////////////////////////////////////////////////////////////////
	/* * Supporting Functions * */
	// //////////////////////////////////////////////////////////////////////////////////

	public static BufferedWriter createFile(String outTxt) {
		try {
			FileWriter fstream = new FileWriter(outTxt);
			BufferedWriter out = new BufferedWriter(fstream);
			return out;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void deleteDir(File dir) {
		if (dir.isDirectory())
			for (File file : dir.listFiles())
				deleteDir(file);
		else
			dir.delete();
	}

	public static Boolean DirExists(String DirPath) {
		return ((new File(DirPath)).exists() ? true : false);
	}
}