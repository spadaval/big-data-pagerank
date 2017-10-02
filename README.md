# big-data-pagerank

Hadoop-based pagerank computation system.

#Overview

Each folder builds to a single jar, defining a single hadoop job.

# Jobs

## `Edge Count` takes the file in the format:
```
src_id dst_id
```	  
And computes the number of edges extending from each node, stored in the format:
```
src_id num_nodes
```

## `Matrix builder` takes the file in the format:

```
src_id dst_id
```	  
and the file in the format:
```
src_id num_nodes
```
And produces a weighted matrix `A`.

## `Matrix multiplier` and `pr` takes the matrix `A` and a pagerank vector `V`, and performs the update rule:

```
V <- A*V
```

# HDFS file structure

```
/pagerank/
         /Input             <-- Stores the given inputs
               /Edges       <-- Stores a listing of all the directed edges
               /Vertices    <-- Stores a listing of id hostname pairs
         /Files             <-- Stores intermediate results and internal computations
               /EdgeCount   <-- Stores a listing of `id edgecount` for each page
               /VertexCount <-- Stores the number of vertices, as a single numeric
               /Output1     <-- Stores the intermediate output from matrixmultiplier
         /Output
               /A           <-- Stores the weightage matrix
               /V           <-- Stores the (output) pagerank weightage
               /NewV        <-- Stores the new output from the matrix multiplication operation
```
