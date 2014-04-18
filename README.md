Partition-based Similarity Search (PSS)
=======================================
  A Partition-based Similarity Search as described in [1][2]. The package takes an input of the format <DocID: word1 word2..> as bag of words and output the pair of document IDs that have a Cosine-based similarity value >= threshold. The framework used is the Java-based MapReduce framework provided by Apache Hadoop. 

Installation:
-------------
1) clone the repository as: 

2) make sure 'ant','java','javac' are installed by typing them into the command line.

3) make sure you have hadoop setup following[ http://hadoop.apache.org/docs/r0.19.0/quickstart.html#Download ]

Package overview:
-----------------
README
src/:
build/:
conf/:
target/:
conf/lib:


Quick start:
------------

1) Copy the project: git clone git@github.com:mahaucsb/pss.git
2) Setup the following environment variables:
   - HADOOP_HOME: to the location of your hadoop directory. Eg.$ export HADOOP_HOME=/home/maha/hadoop-1.0.1
   - JAVA_HOME: 
   
      For Linux: export JAVA_HOME=/usr/lib/jvm/java-openjdk //this is just an example

      For MAC: export JAVA_HOME=/System/Library/Frameworks/JavaVM.framework/Versions/1.6/Home

   - JAVA_VERSION:
   
      For MAC: export JAVA_VERSION=`java -version 2>&1 | head -n 1 | cut -d\" -f 2 | cut -f1 -f2 -d"."`

      In linux: export JAVA_VERSION=`java -version 2>&1| head -n 1 | cut -d \" -f 2 | cut -d . -f1,2`
      
      Else: export JAVA_VERSION=1.6   //depending on your java version
3)  


Configurations:
---------------


Dataset:
--------


Solutions to common errors:
----------------------------


References:
-----------

[1]  "Optimizing Parallel Algorithms for All Pairs Similarity Search".M.Alabduljalil,X.Tang,T.Yang.WSDM'13.

[2]  "Cache-Conscious Performance Optimization for Similarity Search".M.Alabduljalil,X.Tang,T.Yang.SIGIR'13.
