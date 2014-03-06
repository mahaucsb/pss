#!/bin/bash

if [ $# -ne 4 ]
then
  echo "Usage: `basename $0` <sign> <numDocuments> <cluedata=1,tweets=2,emails=3,ymusic=4,gnews=5> <partition? Y/N>"
  exit 4
fi

sign=$1
numDocs=$2
benchmark=$3
part=echo $4 | tr '[:lower:]' '[:upper:]'

############################################################
# Environment Variables Set
############################################################
if [ -z ${HADOOP_HOME} ] || [ -z ${JAVA_VERSION} ]
then
    echo "ERROR: either HADOOP_HOME or JAVA_VERSION is not set."
    exit 0
fi
############################################################

xmlconf=../../conf/hybrid/conf.xml
hybridjar=../target/hybrid.jar
run_hadoop=${HADOOP_HOME}/bin/hadoop

############################################################
# Run Partitioning 
############################################################
cd ../partition
if [ $part == "Y" ] ; then 
    echo "run partitioning"
   # ./run.sh $sign $numDocs $benchmark 
fi
exit
############################################################
# Run Similarity Comparison
############################################################
cd ../hybrid
ant
$run_hadoop jar $hybridjar -conf $xmlconf $sign 


