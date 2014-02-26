#!/bin/bash

if [ $# -ne 4 ]
then
  echo "Usage: `basename $0` <sign> <numDocuments> <numPartitions> <cluedata=1,tweets=2,emails=3,ymusic=4,gnews=5>"
  exit 4
fi

sign=$1
numDocs=$2
numPartitions=$3
cluein=$4

xmlconf=../../src/main/resources/hybrid/conf.xml
hybridjar=../../target/hybrid.jar
HADOOP=$HADOOP_HOME/bin/hadoop
RUN_HOME=`pwd`


#### hashing + sequencing + partitioning
cd ../partitoining
if [ $numPartitions -ne 0 ] 
then 
    cd ../partitoining
    ./run.sh $sign $numDocs $numPartitions $cluein 4
fi

finalpart="staticpartitions"$sign

#### similarity computation
cd $RUN_HOME
ant
$HADOOP jar $hybridjar -conf $xmlconf $sign 


