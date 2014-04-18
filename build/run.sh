#!/bin/bash

if [ $# -ne 3 ]
then
  echo "Usage: `basename $0` <numDocuments> <cluedata=1,tweets=2,emails=3,ymusic=4,gnews=5,,wiki=6,disease=7> <static partitioning? Y/N>"
  exit 1
fi

cd hybrid
./run.sh $1 $2 $3 
cd ..


