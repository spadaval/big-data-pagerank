#!/bin/bash
#clean up the current edgecount file
echo "Cleaning up previous file..."
hadoop fs -rm -r /pagerank/Files/EdgeCount > /dev/null 2>&1
echo "done"

echo "Running edgecount task..."
#run the job
hadoop jar edgecount.jar EdgeCount > /dev/null  2>&1

echo "Complete. File in /pagerank/Files/EdgeCount"
