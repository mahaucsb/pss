#!/bin/bash

if [ $# -ne 4 ]
then
  echo "Usage: `basename $0` <sign> <numDocuments> <cluedata=1,tweets=2,emails=3,ymusic=4,gnews=5> <partition? Y/N>"
  exit 1
fi

sign=$1
numDocs=$2
benchmark=$3
part=`echo $4 | tr '[:lower:]' '[:upper:]'`

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
    ./run.sh $sign $numDocs $benchmark 
else
    echo "#####################################################################"
    echo "WARNING: You should have staticpartitions$sign in your HDFS already!"
    $run_hadoop dfs -ls staticpartitions$sign > /dev/null
    if [ $? -gt 0 ]; then
	echo -e "Error: You should either:\n 1)Rerun and set 'partition=Y' \n 2)Run 'bin/hadoop dfs -cp seqvectors$sign staticpartitions$sign'"
	exit 3 
    fi
fi

############################################################
# Run Similarity Comparison
############################################################
cd ../hybrid
ant
$run_hadoop jar $hybridjar -conf $xmlconf $sign 


