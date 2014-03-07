#!/bin/bash


if [ $# -ne 3 ]
then
  echo "Usage: `basename $0` <sign> <numDocuments> <cluedata=1,tweets=2,emails=3,ymusic=4,gnews=5,wiki=6>"
  exit 3
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
sign=$1
numdocs=$2
benchmark=$3
xmlconf=../../conf/partitioning/conf.xml
partjar=../../target/partitioning.jar
run_hadoop=${HADOOP_HOME}/bin/hadoop

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
    elif [ $benchmark -eq 5 ]  ## gnew
    then
	./rung.sh $sign $numdocs  
    else 
	./runw.sh $sign $numdocs  ##wiki
    fi
fi

############################################################
# Run Partitioning
############################################################
cd ../partition
ant
$run_hadoop jar $partjar cpartitionn -conf $xmlconf $sign 

