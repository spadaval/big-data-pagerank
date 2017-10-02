#!/bin/bash
#clean up the current edgecount file
hadoop fs -rmr /pagerank/Files/VertexCount

#run the job
hadoop jar vertexcount.jar VertexCount

hdfs dfs -get /pagerank/Files/VertexCount ./VertexCount
