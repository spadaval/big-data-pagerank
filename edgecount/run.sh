#!/bin/bash
#clean up the current edgecount file
hadoop fs -rm -r /pagerank/Files/EdgeCount
echo "Running edgecount task"
#run the job
hadoop jar edgecount.jar EdgeCount

echo "Complete. File in /pagerank/Files/EdgeCount"
