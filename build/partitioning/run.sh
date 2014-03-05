#!/bin/bash


if [ $# -ne 3 ]
then
  echo "Usage: `basename $0` <sign> <numDocuments> <cluedata=1,tweets=2,emails=3,ymusic=4,gnews=5>"
  exit 3
fi

sign=$1
numdocs=$2
benchmark=$3


xmlconf=../../src/main/resources/partitioning/conf.xml
partjar=../../target/partitioning.jar
run_hadoop=$HADOOP_HOME/bin/hadoop


############################## Produce sequence file
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
cd ../partitioning
ant
$HADOOP jar $partjar cpartitionn -conf $xmlconf $sign 


