#!/bin/bash


if [ $# -ne 1 ]
then
  echo "Usage: `basename $0` <number of documents>"
  exit 2
fi

############################################################                                                                                                
# Environment Variables Set
############################################################                                                                                                      
if [ -z ${HADOOP_HOME} ] || [ -z ${JAVA_VERSION} ]
then
    echo "ERROR: either HADOOP_HOME or JAVA_VERSION is not set."
    exit 0
fi

############################################################
# Configuration Set
############################################################

numdocs=$1
wikidata=../../data/wiki/10k_1304_A.txt #500-wiki
jarfile=../../target/preprocessing.jar
xmlconf=../../conf/preprocess/conf.xml
tmpdata=input_dir
run_hadoop=${HADOOP_HOME}/bin/hadoop

############################################################
# Copy n Documents to Input Folder
############################################################


rm -r $tmpdata 2>/dev/null
mkdir $tmpdata
head -n $numdocs $wikidata > $tmpdata/input
if [ $? -ne 0 ]
then
  exit 2
fi

############################################################                                                                                        
# Run Preprocessing                                                                                                                                 
###########################################################                                                                                               

echo "*****************************************************************************"
echo "Load "$numdocs" vectors of Wiki data into HDFS"
$run_hadoop dfs -rmr textpages
$run_hadoop dfs -put $tmpdata textpages

echo "convert vectors into numeric text vectors ie (docid num1 num2 num3..)" ###not sure
$run_hadoop jar $jarfile hash -conf $xmlconf

echo "convert vectors into hadoop binray (ie.sequence) of class (Long id, FeatureWeight array)"
$run_hadoop jar $jarfile seq write

echo "Optional: remove unecessary folders from HDFS."
$run_hadoop dfs -rmr hashedvectors
$run_hadoop dfs -rmr features

