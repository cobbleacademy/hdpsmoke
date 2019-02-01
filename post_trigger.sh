#!/bin/bash
# preStep example bash script
#echo "Hello World"
#echo "FILE_DATE=`hadoop fs -cat {HDFS_TMP_FILE} | awk -F '[|.]' '{print $1}'`"
#echo "FILE_NAME=sales-sample_20181222_00.csv"
#echo "FILE_DATE=2018-12-22"
#echo "FILE_HR=00"
#hdfs dfs -touchz /Users/nnagaraju/IDEAWorkspace/drake/output/trigger/trigger_mbr_run_1.done
touch /Users/nnagaraju/IDEAWorkspace/drake/output/trigger/trigger_mbr_run_1.done
touch /Users/nnagaraju/IDEAWorkspace/drake/output/trigger/trigger_mbr_run_2.done
touch /Users/nnagaraju/IDEAWorkspace/drake/output/trigger/trigger_mbr_run_3.done
touch /Users/nnagaraju/IDEAWorkspace/drake/output/trigger/trigger_mbr_run_4.done
touch /Users/nnagaraju/IDEAWorkspace/drake/output/trigger/trigger_mbr_run_5.done
echo "success=true"