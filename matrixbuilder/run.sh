
hdfs dfs -rm -r /pagerank/Output/A 

echo "Running matrixbuilder task"
#run the job
hadoop jar matrixbuilder.jar MatrixBuilder

echo "Complete. File /pagerank/Output/A should have been produced"
