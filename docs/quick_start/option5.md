<a id="getting-started-by-installing-snappydata-on-premise"></a>
##Option 5: Getting Started by Installing SnappyData On-Premise
Download the latest version of SnappyData from the [SnappyData Release Page](https://github.com/SnappyDataInc/snappydata/releases/) page, which lists the latest and previous releases of SnappyData.

```bash
$ tar -xzf snappydata-0.7-bin.tar.gz
$ cd snappydata-0.7-bin/
# Create a directory for SnappyData artifacts
$ mkdir quickstartdatadir
$./bin/spark-shell --conf spark.snappydata.store.sys-disk-dir=quickstartdatadir --conf spark.snappydata.store.log-file=quickstartdatadir/quickstart.log
```
It opens a Spark Shell. All SnappyData metadata as well as persistent data is stored in the directory **quickstartdatadir**. Follow the steps mentioned [here](#Start_quickStart)
