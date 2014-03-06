#!/bin/bash

if [ $# -ne 3 ]
then
  echo "Usage: `basename $0` <sign> <numDocuments> <cluedata=1,tweets=2,emails=3,ymusic=4,gnews=5>"
  exit 3
fi

sign=$1
numDocs=$2
benchmark=$3

############################################################
# CHECK: Environment Variables Set
############################################################
if [ -z ${HADOOP_HOME} ]
then
    echo "ERROR: HADOOP_HOME is not set."
fi
############################################################

xmlconf=../../conf/hybrid/conf.xml
hybridjar=../target/hybrid.jar
run_hadoop=${HADOOP_HOME}/bin/hadoop

############################################################
# Run Partitioning 
############################################################
cd ../partition
if [ $numPartitions -ne 0 ] ; then 
    ./run.sh $sign $numDocs $benchmark 
fi

############################################################
# Run Similarity Comparison
############################################################
cd ../hybrid
ant
$run_hadoop jar $hybridjar -conf $xmlconf $sign 


