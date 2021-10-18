# The Technology Powering <!--iSight Cloud-->SnappyData CloudBuilder

 <!--iSight Cloud-->SnappyData CloudBuilder uses the SnappyData Synopsis Engine to deliver blazing fast responses to queries that have long processing times. Analytic queries typically aim to provide aggregate information and involve full table or partial table scans. The cost of these queries is directly proportional to the amount of data that needs to be scanned. Analytics queries also often involve distributed joins of a dimension table with one or more fact tables. The cost of pruning these queries down to the final result is directly proportional to the size of the data involved. Distributed joins involve lots of data movement making such queries extremely expensive in traditional systems that process the entire data set.

The Synopsis Data Engine offers a breakthrough solution to these problems by building out stratified samples of the most common columns used in queries, as well as other probabilistic data structures like count-min-sketch, bloom filters etc. The use of these structures, along with extensions to the querying engine allow users to get almost-perfect answers to complex queries in a fraction of the time it used to take to answer these queries.

For more information on SDE and sampling techniques used by SnappyData, refer to the [SDE documentation](../sde/index.md).
