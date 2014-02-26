#!/bin/bash

#############################################
# To setup your configrations change the 
# values in this file then run it as:
# ./setConfigurations.sh
# Then run APSS. 
#############################################

INSTAL_PATH=~/cpc/cpc-apss #~/projects/cpc7
CONF_PATH=$INSTAL_PATH/src/main/resources


#PREPROCESSING
pre_maxfreq_feature=1000
pre_option_num=3  #(1)collect features (2) featureVectors (3) featureWeightVectors (5) bag of hashed words
pre_binary_weights=false #ignore feature frequency
pre_lonely_features=true
pre_md5_hash=false
pre_dfcut_ratio=0.05

#PARTITIONING
part_sim_threshold=1
part_num_partitions=3
part_uniform=false
sort_num_reducers=5
sort_p_norm=1
max_doc_length=1200
max_doc_norm=10
max_doc_weight=0.2
part_load_balance=12

#HYBRID
similarity_threshold=$part_sim_threshold
alg_number=0 #0,1,3
hybrid_split_size=20000
hybrid_io_block_size=100
hybrid_comp_block_size=100
hybrid_multiple_s=false 
hybrid_number_multiple_s=7 
hybrid_enable_static_partition=true
hybrid_circular_enabled=true
hybrid_exclude_myself=false
hybrid_single_map=false
hybrid_print_log=true
hybrid_load_balance=$part_load_balance

#PREPROCESSING
xmlconf=$CONF_PATH/preprocess/conf.xml
cp $xmlconf x
sed -e '5 c\
  <value>'$pre_maxfreq_feature'</value>' x > y
sed -e '10 c\
  <value>'$pre_option_num'</value>' y > x
sed -e '16 c\
  <value>'$pre_binary_weights'</value>' x > y
sed -e '21 c\
  <value>'$pre_lonely_features'</value>' y > x
sed -e '31 c\
  <value>'$pre_md5_hash'</value>' x > y
sed -e '36 c\
  <value>'$pre_dfcut_ratio'</value>' y > x
mv x $xmlconf

#PARTITIONING
xmlconf=$CONF_PATH/partitioning/conf.xml
cp $xmlconf x
sed -e '5 c\
  <value>'$part_sim_threshold'</value>' x > y
sed -e '9 c\
  <value>'$part_num_partitions'</value>' y > x
sed -e '14 c\
  <value>'$part_uniform'</value>' x > y
sed -e '20 c\
  <value>'$sort_num_reducers'</value>' y > x
sed -e '24 c\
  <value>'$sort_p_norm'</value>' x > y
sed -e '28 c\
  <value>'$max_doc_length'</value>' y > x
sed -e '33 c\
  <value>'$max_doc_norm'</value>' x > y
sed -e '38 c\
  <value>'$max_doc_weight'</value>' y > x
sed -e '49 c\
  <value>'$part_load_balance'</value>' x > y
mv y $xmlconf


#Hybrid
xmlconf=$CONF_PATH/hybrid/conf.xml
cp $xmlconf x
sed -e '5 c\
  <value>'$similarity_threshold'</value>' x > y
sed -e '11 c\
  <value>'$alg_number'</value>' y > x
sed -e '29 c\
  <value>'$hybrid_split_size'</value>' x > y
sed -e '36 c\
  <value>'$hybrid_io_block_size'</value>' y > x
sed -e '41 c\
  <value>'$hybrid_comp_block_size'</value>' x > y
sed -e '48 c\
  <value>'$hybrid_multiple_s'</value>' y > x
sed -e '53 c\
  <value>'$hybrid_number_multiple_s'</value>' x > y
sed -e '59 c\
  <value>'$hybrid_enable_static_partition'</value>' y > x
sed -e '70 c\
  <value>'$hybrid_circular_enabled'</value>' x > y
sed -e '104 c\
  <value>'$hybrid_exclude_myself'</value>' y > x
sed -e '109 c\
  <value>'$hybrid_single_map'</value>' x > y
sed -e '114 c\
  <value>'$hybrid_print_log'</value>' y > x
sed -e '119 c\
  <value>'$hybrid_load_balance'</value>' x > y
mv y $xmlconf
