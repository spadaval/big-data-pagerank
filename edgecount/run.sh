#!/bin/bash
#clean up the current edgecount file
hadoop fs -rmr /pagerank/Files/EdgeCount

#run the job
hadoop jar edgecount.jar EdgeCount
