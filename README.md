# big-data-pagerank
Hadoop-based pagerank computation system.

Each folder builds to a single jar, descibing a single hadoop job.

*Matrix builder* takes the file in the format:

```
src_id dst_id
```	  

And produces a weighted matrix `A`.

*Matrix multiplier* takes the matrix `A` and a pagerank vector `V`, and performs the update rule:

```
V <- A*V
```

