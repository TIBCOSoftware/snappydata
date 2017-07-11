##  Important Settings 
<a id="buckets"></a>
### Buckets

Bucket is the unit of partitioning for SnappyData tables. The data is distributed evenly across all the buckets. When a new server joins or an existing server leaves the cluster, the buckets are moved around for rebalancing. 

The number of buckets should be set according to the table size. By default, there are 113 buckets for a table. 
If there are more buckets in a table than required, it means there is fewer data per bucket. For column tables, this may result in reduced compression that SnappyData achieves with various encodings. 
Similarly, if there are not enough buckets in a table, not enough partitions are created while running a query and hence cluster resources are not used efficiently.
Also, if the cluster is scaled at a later point of time rebalancing may not be optimal.

For column tables, it is recommended to set a number of buckets such that each bucket has at least 100-150 MB of data.  

### member-timeout

SnappyData efficiently uses CPU for running OLAP queries. In such cases, due to the amount of garbage generated, JVMs garbage collection can result in a system pause. These pauses are rare and can also be minimized by setting the off-heap memory. </br>
For such cases, it is recommended that the `member-timeout` should be increased to a higher value. This ensures that the members are not thrown out of the cluster in case of a system pause.</br>
The default value of `member-timeout` is: 5 sec. 

### spark.local.dir  

SnappyData writes table data on disk.  By default, the disk location that SnappyData uses is the directory specified using `-dir` option, while starting the member. 
SnappyData also uses temporary storage for storing intermediate data. The amount of intermediate data depends on the type of query and can be in the range of the actual data size. </br>
To achieve better performance, it is recommended to store temporary data on a different disk than the table data. This can be done by setting the `spark.local.dir` parameter.