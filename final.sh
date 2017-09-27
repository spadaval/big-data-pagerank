#!/bin/bash
hadoop fs -rmr /pagerank/edgecount
#edge_counter
hadoop com.sun.tools.javac.Main WordCount.java
jar cf wc.jar WordCount*.class
hadoop fs -rmr /pagerank/edgecount
hadoop jar wc.jar WordCount /pagerank/File/inp01.txt /pagerank/edgecount/

hadoop fs -mv /pagerank/edgecount/part-r-* /pagerank/File/edgecount.txt

#
