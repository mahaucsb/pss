#!/bin/bash


if [ $# -ne 2 ]
then
  echo "Usage: `basename $0` <numDocuments> <cluedata=1,tweets=2,emails=3,ymusic=4,gnews=5,wiki=6,disease=7>"
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
numdocs=$1
benchmark=$2
xmlconf=../../conf/partitioning/conf.xml
partjar=../../target/partitioning.jar
run_hadoop=${HADOOP_HOME}/bin/hadoop

############################################################
# Run Preprocessing
############################################################
cd ../preprocess

if [[ $benchmark -ne 0 && $numdocs -ne 0 ]] 
then 

    if [ $benchmark -eq 1 ] ##Clueweb
    then 
	bash runc.sh $numdocs 
    elif [ $benchmark -eq 2 ] ##Twitter
    then
	bash runt.sh $numdocs 
    elif [ $benchmark -eq 3 ]   ## emails
    then 
        bash rune.sh $numdocs 
    elif [ $benchmark -eq 4 ]  ## ymusic
    then
	bash runym.sh $numdocs 
    elif [ $benchmark -eq 5 ]  ## gnew
    then
	bash rung.sh $numdocs  
    elif [ $benchmark -eq 6 ]  ## wiki
    then
	bash runw.sh $numdocs 
    else 
	bash rund.sh $numdocs  ##disease
    fi
fi

############################################################
# Run Partitioning
############################################################
cd ../partition
$run_hadoop jar $partjar -conf $xmlconf

