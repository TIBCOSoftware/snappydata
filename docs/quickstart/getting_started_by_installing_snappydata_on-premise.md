<a id="getting-started-by-installing-snappydata-on-premise"></a>
# Getting Started by Installing SnappyData On-Premise
Download the latest version of SnappyData from the [SnappyData Release Page](https://github.com/SnappyDataInc/snappydata/releases/), which lists the latest and previous releases of SnappyData.

```pre
$ tar -xzf snappydata-1.1.0-bin.tar.gz
$ cd snappydata-1.1.0-bin/
# Create a directory for SnappyData artifacts
$ mkdir quickstartdatadir
$./bin/spark-shell --conf spark.snappydata.store.sys-disk-dir=quickstartdatadir --conf spark.snappydata.store.log-file=quickstartdatadir/quickstart.log
```

It opens the Spark shell. All SnappyData metadata, as well as persistent data, is stored in the directory **quickstartdatadir**.

The spark-shell can now be used to work with SnappyData using [SQL](using_sql.md) and [Scala APIs](using_spark_scala_apis.md).
