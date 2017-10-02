#!/bin/sh
#get the preliminary statistics files ready
cd edgecount
./run.sh
cd ..

cd vertexcount
./run.sh
cd ..
rm VertexCount
hdfs dfs -get /pagerank/Files/VertexCount/part* ./VertexCount

NUM_VERTICES=$(cat VertexCount)


#build the matrix A
cd matrixbuilder
./run.sh
cd ..


#repeatedly run the multiply task
for i in 1 2 3 4 5
do
  cd matrixmultiplier
  ./run.sh $NUM_VERTICES
  cd ..
  cd pr
  ./run.sh
  cd ..
  echo "Removing previous final output..."
  hdfs dfs -rm -r "/pagerank/Output/V" # > /dev/null 2>&1
  echo "done."
  echo "Moving latest outputs..."
  hdfs dfs -mv "/pagerank/Output/NewV" "/pagerank/Output/V" #> /dev/null 2>&1
  hdfs dfs -rm -r "/pagerank/Output/NewV" #> /dev/null 2>&1
  echo "done."
done
