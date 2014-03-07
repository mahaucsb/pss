#!/bin/bash


if [ $# -ne 2 ]
then
  echo "Usage: `basename $0` <sign> <number of documents>"
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

sign=$1
numdocs=$2
wikidata=../../data/wiki/500-wiki
jarfile=../../target/preprocessing.jar
xmlconf=../../conf/preprocess/conf.xml
tmpdata=./data
run_hadoop=${HADOOP_HOME}/bin/hadoop

############################################################
# Copy n Documents to Input Folder
############################################################

ant
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
echo "Load "$numdocs" vectors of Clueweb data into HDFS"
$run_hadoop dfs -rmr textpages$sign
$run_hadoop dfs -put $tmpdata textpages$sign

echo "convert vectors into numeric text vectors ie (docid num1 num2 num3..)" ###not sure
$run_hadoop jar $jarfile hash -conf $xmlconf $sign

echo "convert vectors into hadoop binray (ie.sequence) of class (Long id, FeatureWeight array)"
$run_hadoop jar $jarfile seq write $sign

echo "Optional: remove unecessary folders from HDFS."
$run_hadoop dfs -rmr hashedvectors$sign
$run_hadoop dfs -rmr features$sign
