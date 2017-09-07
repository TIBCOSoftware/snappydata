
<a id="getting-started-with-your-spark-distribution"></a>
## Getting Started with your Spark Distribution

If you are a Spark developer and already using Spark 2.1.1 the fastest way to work with SnappyData is to add SnappyData as a dependency. For instance, using "package" option of Spark Shell.

**Open a command terminal and go to the location of the Spark installation directory:**
```bash
$ cd <Spark_Install_dir>
# Create a directory for SnappyData artifacts
$ mkdir quickstartdatadir
$ ./bin/spark-shell --conf spark.snappydata.store.sys-disk-dir=quickstartdatadir --conf spark.snappydata.store.log-file=quickstartdatadir/quickstart.log --packages "SnappyDataInc:snappydata:0.9-s_2.11"
```

This opens a Spark Shell and downloads the relevant SnappyData files to your local machine. Depending on your network connection speed, it may take some time to download the files. 
All SnappyData metadata, as well as persistent data, is stored in the directory **quickstartdatadir**.

<a id="Start_quickStart"></a>
In this document, it is assumed that you are either familiar with Spark or SQL (not necessarily both). Basic database capabilities like working with Columnar and Row-oriented tables, querying and updating these tables is showcased.

Tables in SnappyData exhibit many operational capabilities like disk persistence, redundancy for HA, eviction, etc. For more information, you can refer to the [detailed documentation](../programming_guide.md#ddl). 

While SnappyData supports Scala, Java, Python, SQL APIs for this quick start you can choose to work with Scala APIs or SQL depending on your preference.

