#!/bin/bash


if [ $# -ne 5 ]
then
  echo "Usage: `basename $0` <sign> <numDocuments> <numPartitions> <cluedata=1,tweets=2,emails=3,ymusic=4,gnews=5> <p>"
  exit 5
fi

sign=$1
numdocs=$2
numpart=$3
choicedata=$4
p=$5


original_conf=../../src/main/resources/partitioning/conf.xml
xmlconf=../../src/main/resources/partitioning/myConf.xml
partjar=../../target/partitioning.jar
HADOOP=$HADOOP_HOME/bin/hadoop
BUILD_HOME=`pwd`


sed -e '9 c\
  <value>'$numpart'</value>' $original_conf > x
sed -e '24 c\
  <value>'$p'</value>' x > $xmlconf


############################## Produce sequence files

if [ $choicedata -ne 0 ] 
then 

    cd ../preprocess/
    if [ $choicedata -eq 1 ] ##Clueweb
    then 
	./runc.sh $sign $numdocs 
    elif [ $choicedata -eq 2 ] ##Twitter
    then
	./runt.sh $sign $numdocs 
    elif [ $choicedata -eq 3 ]   ## emails
    then 
	./rune.sh $sign $numdocs 
    elif [ $choicedata -eq 4 ]  ## ymusic
    then
	./runym.sh $sign $numdocs 
    else 
	./rung.sh $sign $numdocs  ##gnews
    fi
fi

############################## Partition ..
cd $BUILD_HOME
ant

$HADOOP jar $partjar cpartitionn -conf $xmlconf $sign 
#$HADOOP jar $partjar cpartitiona -conf $xmlconf $sign  

#echo " output: jaccard-partitions"$sign
#$HADOOP jar $partjar jpartition -conf $xmlconf length-sorted$sign jaccard-partitions$sign  

finalpart="staticpartitions"$sign


