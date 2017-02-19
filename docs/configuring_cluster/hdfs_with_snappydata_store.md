
## HDFS with SnappyData Store

If using SnappyData store persistence to Hadoop as documented [here](http://gemfirexd.docs.pivotal.io/docs-gemfirexd/disk_storage/persist-hdfs.html), then add the [hbase jar](http://search.maven.org/#artifactdetails|org.apache.hbase|hbase|0.94.27|jar) explicitly to CLASSPATH. The jar is now packed in the product tree, so that can be used or download from Apache Maven. Then add to conf/spark-env.sh:

```export SPARK_DIST_CLASSPATH=</path/to/>hbase-0.94.27.jar```

Substitute the actual path for `</path/to/>` above