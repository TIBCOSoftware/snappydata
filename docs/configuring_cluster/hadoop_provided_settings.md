## Hadoop Provided Settings
If you want to run SnappyData with an already existing custom Hadoop cluster like MapR or Cloudera you should download Snappy without Hadoop from the download link.
This allows you to provide Hadoop at runtime.

To do this you need to put an entry in $SNAPPY-HOME/conf/spark-env.sh as below:
```
export SPARK_DIST_CLASSPATH=$($OTHER_HADOOP_HOME/bin/hadoop classpath)
```

