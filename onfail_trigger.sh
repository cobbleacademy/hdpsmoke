#!/bin/bash
# preStep example bash script
#echo "Hello World"
#echo "FILE_DATE=`hadoop fs -cat {HDFS_TMP_FILE} | awk -F '[|.]' '{print $1}'`"
#echo "FILE_NAME=sales-sample_20181222_00.csv"
#echo "FILE_DATE=2018-12-22"
#echo "FILE_HR=00"
#hdfs dfs -touchz output/trigger/trigger_merge_run.done
touch /Users/nnagaraju/IDEAWorkspace/drake/output/trigger/trigger_merge_run.done
touch /Users/nnagaraju/IDEAWorkspace/drake/output/trigger/trigger_mbr_avail.done
echo "success=true"