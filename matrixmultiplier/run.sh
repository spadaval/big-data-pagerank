
echo "Deleting /pagerank/Files/Output1..."
hdfs dfs -rm -r /pagerank/Files/Output1  > /dev/null 2>&1
echo "done"
echo "running multiply phase 1..."
hadoop jar multiply.jar multiply $1 #> /dev/null  2>&1
echo "done"
