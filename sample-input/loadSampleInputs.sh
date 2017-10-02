
hdfs dfs -rm -r /pagerank/

hdfs dfs -mkdir -p /pagerank/Input
hdfs dfs -mkdir -p /pagerank/Output
hdfs dfs -mkdir -p /pagerank/Files

#Load the inputs
hdfs dfs -put ./Slide19Graph/Input/Edges /pagerank/Input/Edges
hdfs dfs -put ./Slide19Graph/Input/Vertices /pagerank/Input/Vertices

#load the (initial) outputs
hdfs dfs -put ./Slide19Graph/Output/V /pagerank/Output/V
hdfs dfs -put ./Slide19Graph/Output/A /pagerank/Output/A
