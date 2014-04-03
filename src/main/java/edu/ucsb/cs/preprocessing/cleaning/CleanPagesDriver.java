package edu.ucsb.cs.preprocessing.cleaning;

import org.apache.hadoop.util.ProgramDriver;

import edu.ucsb.cs.preprocessing.hashing.HashPagesDriver;
import edu.ucsb.cs.preprocessing.sequence.SeqFilesCombiner;
import edu.ucsb.cs.preprocessing.sequence.SeqWriter;
import edu.ucsb.cs.preprocessing.sequence.SeqReader;

public class CleanPagesDriver {

	/**
	 * Input are files of documents per line. The files are text and can either be compressed via .wrac.gz or not compressed.
	 * MARCH NOT FINISHED
	 * Prints these options to chose from:<br>
	 * - [html] for html pages to be cleaned. <br>
	 * - [wrac] for .wrac.gz files to be cleaned.<br>

	 * @param argv : command line inputs
	 */
	public static void main(String argv[]) {
		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();
		try {
			pgd.addClass("warc", WarcFileCleaner.class,
					"A MapReduce job to clean .warc.gz webpages from html and weird characters into set of features.");
			pgd.addClass(
					"html",
					PageCleaner.class,
					"A MapReduce job to clean html pages from stopwords, weird characters even alphanumerics. It further convert letters into lowercase. ");
			pgd.driver(argv);
		} catch (Throwable e) {
			e.printStackTrace();
		}

		System.exit(exitCode);
	}

}
