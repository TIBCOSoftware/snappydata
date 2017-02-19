<a id="getting-started-with-your-spark-distribution"></a>
##Option 1: Getting Started with your Spark Distribution

If you are a Spark developer and already using Spark 2.0.2,(Core SnappyData is compatible with Spark 2.0.2 version) 
 the fastest way to work with SnappyData is to add SnappyData as a dependency. For instance, using "package" option of Spark Shell.

This section contains instructions and examples using which, you can try out SnappyData in 5 minutes or less. We encourage you to also try out the quick performance benchmark to see the 20X advantage over Spark's native caching performance.


**Open a command terminal and go to the location of the Spark installation directory:**
```bash
$ cd <Spark_Install_dir>
# Create a directory for SnappyData artifacts
$ mkdir quickstartdatadir
$ ./bin/spark-shell --conf spark.snappydata.store.sys-disk-dir=quickstartdatadir --conf spark.snappydata.store.log-file=quickstartdatadir/quickstart.log --packages "SnappyDataInc:snappydata:0.7-s_2.11"
```

This opens a Spark Shell and downloads the relevant SnappyData files to your local machine. Depending on your network connection speed, it may take some time to download the files. 
All SnappyData metadata as well as persistent data is stored in the directory **quickstartdatadir**.

<a id="Start_quickStart"></a>
In this document, we assume you are either familiar with Spark or SQL (not necessarily both). We showcase basic database capabilities like working with Columnar and Row-oriented tables, querying and updating these tables.

Tables in SnappyData exhibit many operational capabilities like disk persistence, redundancy for HA, eviction, etc. For more information, you can refer to the [detailed documentation](../pgm_guide/tables_in_snappydata.md#ddl). 

While SnappyData supports Scala, Java, Python, SQL APIs for this quick start you can choose to work with Scala APIs or SQL depending on your preference.

<a id="getting-started-using-spark-scala-apis"></a>