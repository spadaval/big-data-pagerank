# big-data-pagerank
Hadoop-based pagerank computation system.

Each folder builds to a single jar, descibing a single hadoop job.

*Edge Count* takes the file in the format:
```
src_id dst_id
```	  
And computes the number of edges extending from each node, stored in the format:
```
src_id num_nodes
```

*Matrix builder* takes the file in the format:

```
src_id dst_id
```	  
and the file in the format:
```
src_id num_nodes
```
And produces a weighted matrix `A`.

*Matrix multiplier* takes the matrix `A` and a pagerank vector `V`, and performs the update rule:

```
V <- A*V
```
