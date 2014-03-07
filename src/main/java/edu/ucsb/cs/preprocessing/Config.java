package edu.ucsb.cs.preprocessing;

public class Config {

	public static final String NAMESPACE = "hashing";

	public static final String MAX_FEATURE_FREQ_PROPERTY = NAMESPACE + ".max.freq";
	public static final int MAX_FEATURE_FREQ_VALUE = 1;
	/** Elsayed HLT'08 idea of reducing computation time */
	public static final String DF_CUT_PROPERTY = NAMESPACE + ".df.cut";
	public static final float DF_CUT_VALUE = 0.01f;
	public static final String OPTION_NUMBER_PROPERTY = NAMESPACE + ".option.num";
	public static final int OPTION_NUMBER_VALUE = 3;
	public static final String NUM_REDUCERS_PROPERTY = NAMESPACE + ".num.reducers";
	public static final int NUM_REDUCERS_VALUE = 3;
	public static final String BINARY_WEIGHTS_PROPERTY = NAMESPACE + ".binary.weights";
	public static final boolean BINARY_WEIGHTS_VALUE = false;
	public static final String MD5_HASH_PROPERTY = NAMESPACE + ".md5.hash";
	public static final boolean MD5_HASH_VALUE = false;
	public static final String LONELY_FEATURES_PROPERTY = NAMESPACE + ".lonely.features";
	public static final boolean LONELY_FEATURES_VALUE = false;
	
}
