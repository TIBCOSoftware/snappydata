<a id="getting-started-by-installing-snappydata-on-premise"></a>
# Getting Started by Installing TIBCO ComputeDB On-Premise
Download the latest version of TIBCO ComputeDB from the [TIBCO ComputeDB Release Page](https://github.com/SnappyDataInc/snappydata/releases/), which lists the latest and previous releases of TIBCO ComputeDB.

```pre
$ tar -xzf snappydata-1.0.2.1-bin.tar.gz
$ cd snappydata-1.0.2.1-bin/
# Create a directory for TIBCO ComputeDB artifacts
$ mkdir quickstartdatadir
$./bin/spark-shell --conf spark.snappydata.store.sys-disk-dir=quickstartdatadir --conf spark.snappydata.store.log-file=quickstartdatadir/quickstart.log
```

It opens the Spark shell. All TIBCO ComputeDB metadata, as well as persistent data, is stored in the directory **quickstartdatadir**.

The spark-shell can now be used to work with TIBCO ComputeDB using [Scala APIs](using_spark_scala_apis.md) and [SQL](using_sql.md).
