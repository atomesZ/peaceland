#!/bin/bash

# start ssh server
/etc/init.d/ssh start

# format namenode
$HADOOP_HOME/bin/hdfs namenode -format

# start hadoop
$HADOOP_HOME/sbin/start-dfs.sh

cd $HADOOP_HOME

bin/hdfs dfs -mkdir /user

bin/hdfs dfs -mkdir /user/hadoop

bin/hdfs dfs -put /drone.csv /user/hadoop/drone.csv

# keep container running
tail -f /dev/null
