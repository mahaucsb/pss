#!/bin/bash



if [ $# -ne 2 ]
then
  echo "Usage: `basename $0` <sign> <number of documents>"
  exit 2
fi

RUN_HOME=`pwd`

sign=$1
numdocs=$2

yahoodata=~/data/yahoomusic/trainIdx1-inverted-bag
cluejar=../../target/preprocessing.jar

xmlconf=../../src/main/resources/preprocess/conf.xml
tmpdata=./data
HADOOP=$HADOOP_HOME/bin/hadoop


ant

rm $tmpdata/*
head -n $numdocs $yahoodata > $tmpdata/input

if [ $? -ne 0 ]
then
  exit 2
fi

##### echo "load textfile of the format vector = (word1 word2 word1 word3 ..) per line .."
$HADOOP dfs -rmr textpages$sign
$HADOOP dfs -put $tmpdata textpages$sign

##### echo "convert vectors into numeric text vectors as features/bag depending on option = (docid num1 num2 num3..)"
$HADOOP jar $cluejar hashrecords -conf $xmlconf $sign

##### echo "convert into hadoop sequence (Long id, FeatureWeight array)"
$HADOOP jar $cluejar seqerecords $sign

$HADOOP dfs -rmr hashedvectors$sign
$HADOOP dfs -rmr features$sign

