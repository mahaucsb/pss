package edu.ucsb.cs.preprocessing.sequence;

import org.apache.hadoop.util.ProgramDriver;

import edu.ucsb.cs.preprocessing.cleaning.CleanPagesDriver;
import edu.ucsb.cs.preprocessing.hashing.HashPagesDriver;

public class SequenceDriver {

	/**
	 * Prints these options to chose from:<br>
	 * - [read] read sequence files and print into console. <br>
	 * - [convert] convert text files into sequence files.<br>
	 * 
	 * @param argv : command line inputs
	 */
	public static void main(String argv[]) {
		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();
		try {
			pgd.addClass("write", SeqWriter.class,
					"A MapReduce job to convert hashed pages into sequence files.");
			pgd.addClass("read", SeqReader.class,
					"Print out sequence pages in readable format.");
			pgd.addClass("combine", SeqFilesCombiner.class,
					"A regular java program to combine sequence records from multiple files into one file in hdfs.");
			pgd.driver(argv);
		} catch (Throwable e) {
			e.printStackTrace();
		}

		System.exit(exitCode);
	}

}
