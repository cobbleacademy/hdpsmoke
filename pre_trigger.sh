#!/bin/bash
# preStep example bash script
#echo "Hello World"
#echo "FILE_DATE=`hadoop fs -cat {HDFS_TMP_FILE} | awk -F '[|.]' '{print $1}'`"
#echo "FILE_NAME=sales-sample_20181222_00.csv"
#echo "FILE_DATE=2018-12-22"
#echo "FILE_HR=00"
#hdfs dfs -touchz output/trigger/trigger_merge_run.done
#if [[ $(hdfs dfs -test -e output/trigger/trigger_mbr_avail.done) ]]
if [ -f /Users/nnagaraju/IDEAWorkspace/drake/output/trigger/trigger_mbr_avail.done ] && [ ! -f /Users/nnagaraju/IDEAWorkspace/drake/output/trigger/trigger_merge_run.done ]
then
    rm /Users/nnagaraju/IDEAWorkspace/drake/output/trigger/trigger_mbr_avail.done
    echo "status=MBR trigger Avail exists"
    echo "success=true"
else
    echo "status=Either MBR trigger Avail is missing or Merge trigger RUN exists"
    echo "success=false"
fi