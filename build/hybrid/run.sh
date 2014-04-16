#!/bin/bash

if [ $# -ne 3 ]
then
  echo "Usage: `basename $0` <numDocuments> <cluedata=1,tweets=2,emails=3,ymusic=4,gnews=5,,wiki=6,disease=7> <static partitioning? Y/N>"
  exit 1
fi

numDocs=$1
benchmark=$2
part=`echo $3 | tr '[:lower:]' '[:upper:]'`

############################################################
# Environment Variables Set
############################################################
if [ -z ${HADOOP_HOME} ] || [ -z ${JAVA_VERSION} ]
then
    echo "ERROR: either HADOOP_HOME or JAVA_VERSION is not set."
    exit 2
fi
############################################################

xmlconf=../../conf/hybrid/conf.xml
hybridjar=../../target/hybrid.jar
run_hadoop=${HADOOP_HOME}/bin/hadoop

############################################################
# Run Partitioning and Preprocessing
############################################################
cd ../partition
if [ $part == "Y" ] ; then 
    ./run.sh $numDocs $benchmark 
else
    echo "#####################################################################"
    echo "WARNING: You should have staticpartitions in your HDFS already!"
    $run_hadoop dfs -ls staticpartitions > /dev/null
    if [ $? -gt 0 ]; then
	echo -e "Error: You should either:\n 1)Rerun and set 'partition=Y' \n 2)Run 'bin/hadoop dfs -cp seqvectors staticpartitions'"
	exit 3 
    fi
fi

############################################################
# Run Similarity Comparison
############################################################
cd ../hybrid
$run_hadoop jar $hybridjar -conf $xmlconf 


