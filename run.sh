#!/bin/sh
#get the preliminary statistics files ready

cd sample-input
./loadSampleInputs.sh
cd ..

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
#### EVERYTHING ABOVE THIS WORKS DO NOT EDIT

echo $NUM_VERTICES > ~/mm/n

#repeatedly run the multiply task
exit
'
cd ~/mm/
for i in 1 2 3
do
  ./run $NUM_VERTICES
 # cd ..
 # cd pr
 # ./run.sh
 # cd ..
  echo "Removing previous final output..."
  hdfs dfs -rm -r "/pagerank/Output/V" # > /dev/null 2>&1
  echo "done."
  echo "Moving latest outputs..."
  hdfs dfs -mv "/pagerank/Output/NewV" "/pagerank/Output/V" #> /dev/null 2>&1
  hdfs dfs -rm -r "/pagerank/Output/NewV" #> /dev/null 2>&1
  echo "done."
  hdfs dfs -cat "/pagerank/Output/V/*"
done
'
