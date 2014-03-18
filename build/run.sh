#!/bin/bash

if [ $# -ne 4 ]
then
  echo "Usage: `basename $0` <sign> <numDocuments> <cluedata=1,tweets=2,emails=3,ymusic=4,gnews=5,wiki=6> <partition? Y/N"
  exit 4
fi

bash hybrid/run.sh $1 $2 $3 $4

############################################################
# Environment Variables Set

