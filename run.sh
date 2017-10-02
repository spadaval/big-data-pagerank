#!/bin/sh
#get the preliminary statistics files ready
edgecount/run.sh
vertexcount/run.sh
#build the matrix A
matrixbuilder/run.sh
#repeatedly run the multiply task
for i in {1..5}
do
  matrixmultiplier/run.sh
  pr/run.sh
  hdfs dfs -rm -r "/pagerank/Output/V"
  hdfs dfs -mv "/pagerank/Output/NewV" "/pagerank/Output/V"
done
