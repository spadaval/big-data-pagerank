#!/bin/sh
#get the preliminary statistics files ready
cd edgecount
./run.sh
cd ..

cd vertexcount
./run.sh
cd ..

NUM_VERTICES = $(cat vertexcount/VertexCount)


#build the matrix A
cd matrixbuilder
./run.sh
cd ..


#repeatedly run the multiply task
for i in {1..5}
do
  cd matrixmultiplier
  ./run.sh
  cd ..
  cd pr
  ./run.sh
  cd ..
  hdfs dfs -rm -r "/pagerank/Output/V"
  hdfs dfs -mv "/pagerank/Output/NewV" "/pagerank/Output/V"
done
