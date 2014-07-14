#!/bin/bash

##################################################################
# Set up the default configurations to run 500 documents for
# any of the given datasets. Please check conf.xml files for
# the meaning of each variable.
##################################################################

CONF_PATH=./

#PREPROCESSING
pre_maxfreq_feature=1000           
pre_option_num=3  
pre_binary_weights=false 
pre_lonely_features=false
pre_md5_hash=false
pre_dfcut_ratio=0

#PARTITIONING
part_sim_threshold=0.9
part_sim_metric=cosine
part_num_layers=3
part_uniform=true
sort_num_reducers=5
sort_p_norm=1
max_doc_length=1200
max_doc_norm=10
max_doc_weight=0.2
#part_load_balance=0 # xun: not fully done

#HYBRID
hybrid_sim_metric=cosine
similarity_threshold=$part_sim_threshold
alg_number=1
hybrid_S_size=1000
hybrid_io_block_size=100
hybrid_comp_block_size=100
hybrid_number_multiple_s=7 
hybrid_enable_static_partition=true
hybrid_circular_enabled=false
hybrid_exclude_myself=false
hybrid_single_map=true
hybrid_print_log=false
hybrid_load_balance=$part_load_balance
hybrid_debug_load_balance=false


##################################################################

#PREPROCESSING
xmlconf=./preprocess/conf.xml
cp $xmlconf x
sed -e '6 c\
  <value>'$pre_maxfreq_feature'</value>' x > y
sed -e '12 c\
  <value>'$pre_binary_weights'</value>' y > x
sed -e '18 c\
  <value>'$pre_lonely_features'</value>' x > y
sed -e '24 c\
  <value>'$pre_md5_hash'</value>' y > x
sed -e '30 c\
  <value>'$pre_dfcut_ratio'</value>' x > y
sed -e '42 c\
  <value>'$pre_option_num'</value>' y > x
mv x $xmlconf

#PARTITIONING
xmlconf=./partitioning/conf.xml
cp $xmlconf x
sed -e '5 c\
  <value>'$part_sim_metric'</value>' x > y
sed -e '11 c\
  <value>'$part_sim_threshold'</value>' y > x
sed -e '16 c\
  <value>'$part_num_partitions'</value>' x > y
sed -e '22 c\
  <value>'$part_uniform'</value>' y > x
sed -e '29 c\
  <value>'$sort_num_reducers'</value>' x > y
sed -e '34 c\
  <value>'$sort_p_norm'</value>' y > x
sed -e '39 c\
  <value>'$max_doc_length'</value>' x > y
sed -e '45 c\
  <value>'$max_doc_norm'</value>' y > x
sed -e '51 c\
  <value>'$max_doc_weight'</value>' x > y
mv y $xmlconf

#Hybrid
xmlconf=./hybrid/conf.xml
cp $xmlconf x
sed -e '6 c\
  <value>'$hybrid_sim_metric'</value>' x > y  
sed -e '12 c\
  <value>'$similarity_threshold'</value>' y > x
sed -e '18 c\
  <value>'$alg_number'</value>' x > y
sed -e '38 c\
  <value>'$hybrid_S_size'</value>' y > x
sed -e '46 c\
  <value>'$hybrid_number_multiple_s'</value>' x > y
sed -e '53 c\
  <value>'$hybrid_io_block_size'</value>' y > x
sed -e '59 c\
  <value>'$hybrid_comp_block_size'</value>' x > y
sed -e '67 c\
  <value>'$hybrid_load_balance'</value>' y > x
sed -e '79 c\
  <value>'$hybrid_enable_static_partition'</value>' x > y
sed -e '91 c\
  <value>'$hybrid_circular_enabled'</value>' y > x
sed -e '124 c\
  <value>'$hybrid_exclude_myself'</value>' x > y
sed -e '130 c\
  <value>'$hybrid_single_map'</value>' y > x
sed -e '142 c\
  <value>'$hybrid_print_log'</value>' x > y
mv y $xmlconf
