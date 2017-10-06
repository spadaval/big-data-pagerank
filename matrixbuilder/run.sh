
hdfs dfs -rm -r /pagerank/Output/A  > /dev/null 2>&1

echo "Running matrixbuilder task"
#run the job
hadoop jar matrixbuilder.jar MatrixBuilder #> /dev/null 2>&1

echo "Complete. File /pagerank/Output/A should have been produced"
