Partition-based Similarity Search (PSS)
=======================================
  A Partition-based Similarity Search as described in [1][2]. The package takes an input of the format <DocID: word1 word2..> as bag of words and output the pair of document IDs that have a Cosine-based similarity value >= threshold. The framework used is the Java-based MapReduce framework provided by Apache Hadoop. 

Installation:
-------------
1) clone the repository as: 

2) make sure 'ant','java','javac' are installed by typing them into the command line.

3) 

Package overview:
-----------------
README
src/:
build/:
conf/:
target/:


Quick start:
------------

1) copy conf/ directory to src/main/resources using the command: cp -r conf src/main/resources
2) setup the following environment variables:
   - HADOOP_HOME:
   - PROJECT_HOME:
   - JAVA_HOME:
   - 
conf/lib:
If you're using eClipse, all all the jar files inside this directory to your build path. Otherwise, if you're using the command line then the scripts will do the work to find them.



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
