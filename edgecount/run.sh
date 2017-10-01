#!/bin/bash
hadoop fs -rmr /pagerank/Files/EdgeCount

#edge_counter
hadoop jar wc.jar WordCount 
