package edu.ucsb.cs.partitioning;

import edu.ucsb.cs.sort.SortDriver;

public class Config {

	public static final String NAMESPACE = "partition";
	public static final String METRIC_PROPERTY = NAMESPACE + ".similarity.metric";
	public static final String METRIC_VALUE = "cosine";
	public static final String THRESHOLD_PROPERTY = NAMESPACE + ".similarity.threshold";
	public static final float THRESHOLD_VALUE = 0.7f;
	public static final String NUM_PARTITIONS_PROPERTY = NAMESPACE + ".number.partitions";
	public static final int NUM_PARTITIONS_VALUE = 5;
	public static final String UNIFORM_PARTITIONING_PROPERTY = NAMESPACE + ".uniform.partitions";
	public static final Boolean UNIFORM_PARTITIONING_VALUE = false;
	public static final String PRINT_DISTRIBUTION_PROPERTY = NAMESPACE + ".print.distribution";
	public static final Boolean PRINT_DISTRIBUTION_VALUE = false;
}
