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
 * @Since Jan 7, 2013
 */

package edu.ucsb.cs.hybrid;

/**
 * @author Maha
 * 
 */
public class Config {

	public static final String NAMESPACE = "hybrid";
	public static final String METRIC_PROPERTY = NAMESPACE + ".similarity.metric";
	public static final String METRIC_VALUE = "cosine";
	public static final String BLOCK_SIZE_PROPERTY = NAMESPACE + ".io.block";
	public static final int BLOCK_SIZE_VALUE = 1000;
	public static final String COMP_BLOCK_PROPERTY = NAMESPACE + ".comp.block";
	public static final int COMP_BLOCK_VALUE = 100;
	public static final String THRESHOLD_PROPERTY = NAMESPACE + ".sim.threshold";
	public static final float THRESHOLD_VALUE = 0.7f;
	public static final String STATIC_PARTITION_PROPERTY = NAMESPACE + ".static.partition";
	public static final boolean STATIC_PARTITION_VALUE = true;
	public static final String CIRCULAR_PROPERTY = NAMESPACE + ".circular.enable"; //check: if not enabled what happens?
	public static final boolean CIRCULAR_VALUE = true;
	public static final String PART_DOT_VECTOR_PROPERTY = NAMESPACE + ".partition.dot.vector";
	public static final boolean PART_DOT_VECTOR_VALUE = false;
	public static final String EXCLUDE_MYSELF_PROPERTY = NAMESPACE + ".exclude.myself";
	public static final boolean EXCLUDE_MYSELF_VALUE = false;
	public static final String BAYADRO_SKIP_PROPERTY = NAMESPACE + ".google.skip";
	public static final boolean BAYADRO_SKIP_VALUE = false;
	public static final String MULTI_THREADS_PROPERTY = NAMESPACE + ".multi.threads";
	public static final boolean MULTI_THREADS_VALUE = false;
	public static final String LOG_PROPERTY = NAMESPACE + ".log.enable";
	public static final boolean LOG_VALUE = false;
	public static final String SPLITABLE_PROPERTY = "hadoop.splitting.enabled";
	public static final boolean SPLITABLE_VALUE = false;
	public static final String SPLIT_MB_PROPERTY = "hadoop.split.size";
	public static final Integer SPLIT_MB_VALUE = 64;
	public static final String MAP_S_PROPERTY = NAMESPACE + ".S.size";
	public static final Integer MAP_S_VALUE = 5000;
	public static final String PARTITIONS_DOT_PROPERTY = NAMESPACE + ".dot.partitions";
	public static final boolean PARTITIONS_DOT_VALUE = true;
	public static final String BLOCK_CHOICE_PROPERTY = NAMESPACE + ".block.number";
	public static final Integer BLOCK_CHOICE_VALUE = 1;//1 or 2
	public static final String SINGLE_MAP_PROPERTY = NAMESPACE + ".single.map";
	public static final boolean SINGLE_MAP_VALUE = true;
//	public static final String MULTIPLE_S_PROPERTY = NAMESPACE + ".multiple.s";
//	public static final boolean MULTIPLE_S_VALUE = true;
	public static final String NUMBER_SPLITS_PROPERTY = NAMESPACE + ".number.splits";
	public static final Integer NUMBER_SPLITS_VALUE = 1;
	public static final String LOAD_BALANCE_PROPERTY = NAMESPACE + ".load.balancing";
	public static final Integer LOAD_BALANCE_VALUE = 0;
//	public static final String P_NORM_PROPERTY = NAMESPACE + ".p.norm";
//	public static final Integer P_NORM_VALUE = 1;
	public static final String METWALLY_PROPERTY = NAMESPACE + ".metwally";
	public static final boolean METWALLY_VALUE = true;
	public static String DEBUG_STAGES_PROPERTY = "load.balance.debug";
	public static boolean DEBUG_STAGES_VALUE = true;
	public static final String CONVERT_TEXT_PROPERTY = NAMESPACE + ".text.back";
	public static final boolean CONVERT_TEXT_VALUE = true;

}
