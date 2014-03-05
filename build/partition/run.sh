#!/bin/bash


if [ $# -ne 3 ]
then
  echo "Usage: `basename $0` <sign> <numDocuments> <cluedata=1,tweets=2,emails=3,ymusic=4,gnews=5>"
  exit 3
fi

############################################################
# CHECK: Environment Variables Set
############################################################
if [ -z ${HADOOP_HOME} ]
then
    echo "ERROR: HADOOP_HOME is not set."
fi
############################################################

sign=$1
numdocs=$2
benchmark=$3
xmlconf=../../src/main/resources/partitioning/conf.xml
partjar=../../target/partitioning.jar
run_hadoop=$HADOOP_HOME/bin/hadoop


############################################################
# Run Preprocessing
############################################################
cd ../preprocess

if [ $benchmark -ne 0 ] 
then 

    if [ $benchmark -eq 1 ] ##Clueweb
    then 
	./runc.sh $sign $numdocs 
    elif [ $benchmark -eq 2 ] ##Twitter
    then
	./runt.sh $sign $numdocs 
    elif [ $benchmark -eq 3 ]   ## emails
    then 
	./rune.sh $sign $numdocs 
    elif [ $benchmark -eq 4 ]  ## ymusic
    then
	./runym.sh $sign $numdocs 
    else 
	./rung.sh $sign $numdocs  ##gnews
    fi
fi

############################## Partition ..
cd ../partition
ant
$HADOOP jar $partjar cpartitionn -conf $xmlconf $sign 


#!/bin/bash

if [ $# -ne 4 ]
then
  echo "Usage: `basename $0` <sign> <numDocuments> <numPartitions> <cluedata=1,tweets=2,emails=3,ymusic=4,gnews=5>"
  exit 4
fi

sign=$1
numDocs=$2
numPartitions=$3
benchmark=$4

############################################################
xmlconf=../src/main/resources/hybrid/conf.xml
hybridjar=../target/hybrid.jar
run_hadoop=${HADOOP_HOME}/bin/hadoop

cd ../partitoining
if [ $numPartitions -ne 0 ] 
then 
    cd ../partitoining
    ./run.sh $sign $numDocs $numPartitions $benchmark 
fi

cd ../hybrid
ant
$run_hadoop jar $hybridjar -conf $xmlconf $sign 


