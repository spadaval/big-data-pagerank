#!/bin/bash
hadoop fs -rmr /pagerank/edgecount

#edge_counter
hadoop jar wc.jar WordCount /pagerank/File/inp01.txt /pagerank/File/edgecount/
