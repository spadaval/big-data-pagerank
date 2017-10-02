#!/bin/bash
#clean up the current edgecount file
echo "cleanup.."
hadoop fs -rm -r /pagerank/Files/VertexCount  > /dev/null 2>&1
echo "done"
#run the job
echo "Running vertexcount.."
hadoop jar vertexcount.jar VertexCount > /dev/null 2>&1
echo "done"
