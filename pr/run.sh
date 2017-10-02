echo "removing previous temporary final outputs..."
hdfs dfs -rm -r /pagerank/Output/NewV
echo "done."
echo "running multiplication phase 2..."
hadoop jar color.jar color > /dev/null  2>&1
echo "done."
