# Overview

SnappyData™ is a memory-optimized database based on Apache Spark. It delivers very high
throughput, low latency, and high concurrency for unified analytic workloads that may combine
streaming, interactive analytics, and artificial intelligence in a single, easy to manage distributed cluster.

In previous releases there were two editions namely, the Community Edition which was a fully functional
core OSS distribution that was under the Apache Source License v2.0, and the Enterprise Edition
which was sold by TIBCO Software under the name TIBCO ComputeDB™ that included everything offered
in the OSS version along with additional capabilities that are closed source and only available
as part of a licensed subscription.

The SnappyData team is pleased to announce the availability of version 1.3.0 of the platform
in which all the platform's private modules have been made open-source apart from the streaming
GemFire connector (which includes non-OSS Pivotal GemFire product jars, and hence cannot be open-sourced).
These include Approximate Query Processing (AQP) and the JDBC connector repositories
which also include the off-heap storage support for column tables and the security modules.
In addition, the ODBC driver has also been made open-source. With this, the entire
code base of the platform (apart from the GemFire connector) has been made
open source and there are no longer separate Community and Enterprise editions.

You can find details of the release artifacts towards the end of this page.

The full set of documentation for SnappyData including installation guide, user guide
and reference guide can be found [here](https://tibcosoftware.github.io/snappydata).

The following table summarizes the high-level features available in the SnappyData platform:

| Feature | Available |
| ------- | --------- |
|Mutable Row and Column Store | X |
|Compatibility with Spark | X |
|Shared Nothing Persistence and HA | X |
|REST API for Spark Job Submission | X |
|Fault Tolerance for Driver | X |
|Access to the system using JDBC Driver | X |
|CLI for backup, restore, and export data | X |
|Spark console extensions | X |
|System Performance/behavior statistics | X |
|Support for transactions in Row tables | X |
|Support for indexing in Row tables | X |
|Support for snapshot transactions in Column tables | X |
|Online compaction of column block data | X |
|Transparent disk overflow of large query results | X |
|Support for external Hive meta store | X |
|SQL extensions for stream processing | X |
|SnappyData sink for structured stream processing | X |
|Structured Streaming user interface | X |
|Runtime deployment of packages and jars | X |
|Scala code execution from SQL (EXEC SCALA) | X |
|Out of the box support for cloud storage | X |
|Support for Hadoop 3.2 | X |
|SnappyData Interpreter for Apache Zeppelin | X |
|Synopsis Data Engine for Approximate Querying | X |
|Support for Synopsis Data Engine from TIBCO Spotfire® | X |
|ODBC Driver with High Concurrency | X |
|Off-heap data storage for column tables | X |
|CDC Stream receiver for SQL Server into SnappyData | X |
|Row Level Security | X |
|Use encrypted password instead of clear text password | X |
|Restrict Table, View, Function creation even in user’s own schema | X |
|LDAP security interface | X |
|Visual Statistics Display (VSD) tool for system statistics (gfs) files(*) |  |
|GemFire connector |  |

(*) NOTE: The graphical Visual Statistics Display (VSD) tool to see the system statistics (gfs) files is not OSS
and was never shipped with SnappyData. It is available from [GemTalk Systems](https://gemtalksystems.com/products/vsd/)
or [Pivotal GemFire](https://network.pivotal.io/products/pivotal-gemfire) under their own respective licenses.


## New Features

SnappyData 1.3.0 release includes the following new features over the previous 1.2.0 release:

* **Open sourcing of non-OSS components**<br><br>

  All components except for the streaming GemFire connector are now OSS! This includes the Approximate Querying
  Engine, off-heap storage for column tables, the streaming JDBC connector for CDC, security modules and the ODBC driver.
  All new OSS modules are available under the Apache Source License v2.0 like the rest of the product.
  Overall the new 1.3.0 OSS release is both more feature rich than the erstwhile 1.2.0 Enterprise product,
  and more efficient.<br><br>

* **Online compaction of column block data**<br><br>

  Automatic online compaction of column blocks that have seen significant percentage of deletes or updates.
  The compaction is triggered in one of the foreground threads performing delete or update operations
  to avoid the "hidden" background operational costs of the platform. The ratio of data at which compaction
  is triggered can be configured using two cluster level (or system) properties:
    * _snappydata.column.compactionRatio_: for the ratio of deletes to trigger compaction (default 0.1)
    * _snappydata.column.updateCompactionRatio_: for the ratio of updates to trigger compaction (default 0.2)<br><br>

* **Transparent disk overflow of large query results**<br><br>

  Queries that return large results have been a problem with Spark and SnappyData alike due to lack
  of streaming of the final results to application layer resulting in large heap consumption on the driver.
  There are properties like _spark.driver.maxResultSize_ in Spark to altogether disallow large query results.
  SnappyData has had driver-side persistence of large results for JDBC/ODBC to reduce the memory pressure
  but even so fetching multi-GB results was not possible in most cases and would lead to OOME on driver or
  the server that is servicing the JDBC/ODBC client.<br><br>

  This new feature adds disk overflow for large query results on the executors completely eliminating all
  memory problems for any size of results. It works when using the JDBC/ODBC driver and for the
  SnappySession.sql().toLocalIterator() API. Note that usage of any other Dataset APIs will result in
  creation of base Spark Dataset implementation that will not use disk overflow. A couple of
  cluster level (or system) properties can be used to fine-tune the behaviour:
    * _spark.sql.maxMemoryResultSize_: maximum size of results from a JDBC/ODBC/SQL query in a partition
      that will be held in memory beyond which the results will be written to disk, while the maximum
      size of a single disk block is fixed to 8 times this value (default 4mb)
    * _spark.sql.resultPersistenceTimeout_: maximum duration in seconds for which results overflowed to disk
      are held on disk after which they are cleaned up (default 21600 i.e. 6 hours)<br><br>

* **Eager cleanup of broadcast join data blocks**<br><br>

  The Dataset broadcast join operator uses the Spark broadcast variables to collect required data
  from the executors (when it is within the _spark.sql.autoBroadcastJoinThreshold_ limit) that is cached
  on the driver and executors using the BlockManager. The cleanup of this data uses weak references on
  driver which will be collected in some future GC cycle. When there are frequent queries that perform
  broadcast joins, then this causes large GC pressure on the nodes even though BlockManager will
  overflow data to disk after a point. This is because broadcast join is for relatively small
  data so when those start accumulating for long period of time, they get promoted to old generation
  which will lead to more frequent full GC cycles that need to do quite a bit of work. This is
  especially the case for executors since the cleanup is driven by GC on the driver that may happen
  far more infrequently than the executors, so the GC cycles may fail to clean up old data that
  has not exceeded the execution cache limit.<br><br>

  This new feature eagerly removes broadcast join data blocks at the end of query from the executors
  (which can still fetch from driver on demand) and also from the driver for DML executions.<br><br>

* **Hadoop upgraded to version 3.2.0 and added ADLS gen 1/2 connectivity**<br><br>

  Hadoop upgraded to 3.2.0 from 2.7.x to allow support for newer components like ADLS gen 1/2.
  Azure jars added to the product by default to allow support for ADLS.<br><br>

* **Enable LRUCOUNT eviction for column tables to enable creation of disk-only column tables**<br><br>

  LRUCOUNT based eviction was disabled for column tables since the count would represent column blocks
  per data node while the number of rows being cached in memory would be indeterminate. This is now
  enabled to allow for cases like disk-only tables with minimal memory caching. So now one can create
  a column table like `CREATE TABLE diskTable (...) using column options (eviction_by 'lrucount 1')`
  that will keep only one column block in memory which will be few MB at maximum (in practise only
  few KB since statistics blocks will be preferred for caching). Documentation covers the fact that
  LRUCOUNT based eviction for column tables will lead to indeterminate memory usage.<br><br>

* **Spark layer updated to v2.1.3**<br><br>

  SnappyData Smart Connector will continue to support Apache Spark 2.1.1 as well as later 2.1.x releases.<br><br>

* **JDBC driver now supports Java 11**<br><br>

  The JDBC driver is now compatible with Java 11 and higher till Java 16. It will continue to be
  compatible with Java 8 like the rest of the product.

Apart from the above new features, the interactive execution of Scala code using **EXEC SCALA** SQL
that was marked experimental in the previous release is now considered production ready.


## Stability and Performance Improvements

SnappyData 1.3.0 includes the following stability and performance improvements:

* Bulk send for multiple statement close messages in hive-metastore getAllTables to substantially
  improve metastore performance.<br><br>

* Optimize putInto for small puts using a local cache. (PR#1563)<br><br>

* Increase default putInto join cache size to infinite and allow it to overflow to disk if large.<br><br>

* Switch to safe ThreadUtils await methods throughout the code and merge fixes for [SPARK-13747].
  This fixes potential ThreadLocal leaks in RPC when using ForkJoinPool.<br><br>

* Cleanup to use a common SnapshotConnectionListener to handle snapshot transactions fixing
  cases of snapshots ending prematurely especially with the new column compaction feature.<br><br>

* Add caching of resolved relations in SnappySessionCatalog. This gives a large boost to Spark external
  table queries when the table metadata is large (for example, file based tables having millions of files).<br><br>

* Added boolean _snappydata.sql.useDriverCollectForGroupBy_ property to allow driver do the direct collect
  of partial results for top-level GROUP BY. This avoids the last EXCHANGE for partial results of a GROUP BY
  query improving the performance substantially for sub-second queries. It should be enabled only when the
  final size of results of the query is known to be small else can cause heap memory issues on the driver.<br><br>

* Dashboard statistics collection now cleanly handles cases where the collection takes more time than
  its refresh interval (default 5 seconds). Previously it could cause multiple threads to pile up and
  miss updating results from a delayed collection thread.<br><br>

* Added a pool for UnsafeProjections created in ColumnDelta.mergeStats to reduce the overhead of generated
  code creation and compilation in every update.<br><br>

* For column table update/delete, make the output partitioning of ColumnTableScan to be ordered on bucketId.
  This avoids unnecessary local sort operators being introduced in the update/delete/put plans.<br><br>

* Continue for all SparkListener failures except OutOfMemoryException. This allows the product to continue
  working for cases where custom SparkListener supplied by user throws an exception.<br><br>

* Reduce overheads of EventTracker and TombstoneService to substantially decrease heap usage
  in applications doing continuous update/delete/put operations.<br><br>

* Merged pull request #2 from crabo/spark-multiversion-support to improve OldEtriesCleanThread and hot sync locks.<br><br>

* Removed fsync for DataDictionary that causes major performance issues for large hive metastore updates
  that also uses the DataDictionary. Replication of DataDictionary across all data nodes and locators
  ensures its resilience.<br><br>

* Merged patches for the following Spark issues for increased stability of the product:
  * [SPARK-27065](https://issues.apache.org/jira/browse/SPARK-27065): Avoid more than one active task set managers
    for a stage (causes termination of DAGScheduler and thus the whole JVM).
  * [SPARK-13747](https://issues.apache.org/jira/browse/SPARK-13747): Fix potential ThreadLocal leaks in RPC when
    using ForkJoinPool (can cause applications using ForkJoinPool to fail e.g. for scala's Await.ready/result).
  * [SPARK-26352](https://issues.apache.org/jira/browse/SPARK-26352): Join reorder should not change the order of
    output attributes (can cause unexpected query failures or even JVM SEGV failures).
  * [SPARK-25081](https://issues.apache.org/jira/browse/SPARK-25081): Nested spill in ShuffleExternalSorter
    should not access released memory page (can cause JVM SEGV and related failures).
  * [SPARK-21907](https://issues.apache.org/jira/browse/SPARK-21907): NullPointerException in
    UnsafeExternalSorter.spill() due to OutOfMemory during spill.


## Resolved Issues

SnappyData 1.3.0 resolves the following major issues:

* [SPARK-31918] SparkR support for R 4.0.0+<br><br>

* [SNAP-3306] Row tables with altered schema having added columns causes failure in recovery mode:<br>
  Allowing -1 as value for SchemaColumnId, for the case when the column is missing in the schema-version
  under consideration. This happens when it tries to read older rows, which doesn't have the new column. (PR#1529)<br><br>

* [SNAP-3326] Allow option of replacing the StringType to desired SQL type (VARCHAR, CHAR, CLOB etc)
  when creating table by importing external table. The option `string_type_as = [VARCHAR|CHAR|CLOB]`
  now allows for switching to the required SQL type.<br><br>

* [SDENT-175] Support for cancelling query via JDBC (PR#1539)<br><br>

* [SDENT-171] rand() alias in sub-select is not working. Fixed collectProjectsAndFilters for nondeterministic
  functions in PR#1541.<br><br>

* [SDENT-187] Wrong results returned by queries against partitioned ROW table.
  Fix by handling EqualTo(Attribute, Attribute) case (PR#1544).
  Second set of cases fixed by handling comparisons other than EqualTo. (PR#1546)<br><br>

* [SDENT-199] Fix for select query with where clause showing the plan without metrics in SQL UI tab. (PR#1552)<br><br>

* [SDENT-170] Return JDBC metadata in lower case. (Store PR#550)<br><br>

* [SDENT-185] If connections to ComputeDB is opened, using both hostname and IP address,
  the ComputeDB ODBC driver throws a System.Data.Odbc.OdbcException.<br><br>

* [SDENT-139] Exception in extracting SQLState (when it's not set or improper) masks original problem.<br><br>

* [SDENT-195] Fix NPE by adding a null check for Statement instance before getting its maxRows value.(Store PR#559)<br><br>

* [SDENT-194] Return the table type as just a `table` instead of `ROW TABLE` or `COLUMN TABLE`.(Store PR#560)<br><br>

* [GITHUB-1559] JDBC ResultSet metadata search is not case-insensitive.
  Add upper-case names to search index of ClientResultSet. (PR#565)<br><br>

* [SNAP-3332] PreparedStatement: Fix for precision/scale of DECIMAL parameter types not sent in
  execute causing them to be overwritten by another PreparedStatement. (PR#1562, Store PR#567)<br><br>

* Fix for occasional putInto/update test failures. (Store PR#568)<br><br>

* Native C++ driver (used by the ODBC driver) updated to the latest versions of library dependencies
  (thrift, boost, openssl) with many fixes.<br><br>

* Fixes to the scripts for bind-address and hostname-for-clients auto setup, `hostname -I` not available
  on some systems and others.<br><br>

* Switch eclipse-collections to fastutil due to being more comprehensive and better performance.<br><br>

* [SNAP-3145] Test coverage for external hive metastore.<br><br>

* [SNAP-3321] Example for EXEC SCALA via JDBC.<br><br>

* Expanded CDC and Data Extractor test coverage.


## Known Issues

The following among the known issues have been **fixed** over the previous 1.2.0 release:

| Key | Item | Description |
| --- | ---- | ----------- |
|[SNAP-3306](https://jirasnappydataio.atlassian.net/browse/SNAP-3306) | Row tables with altered schema having added columns causes failure in recovery mode. | A new column that is added in an existing table in normal mode fails to get restored in the recovery mode. |

For the remaining known issues, see the **Known Issues** section of [1.2.0 release notes](https://raw.githubusercontent.com/TIBCOSoftware/snappydata/community_docv1.2.0/docs/release_notes/TIB_compute-ce_1.2.0_relnotes.pdf#%5B%7B%22num%22%3A63%2C%22gen%22%3A0%7D%2C%7B%22name%22%3A%22XYZ%22%7D%2C69%2C720%2C0%5D).
Note that the issue links in that document having https://jira.snappydata.io are no longer valid which should be
changed to https://jirasnappydataio.atlassian.net. For example the broken https://jira.snappydata.io/browse/SNAP-3298
becomes https://jirasnappydataio.atlassian.net/browse/SNAP-3298.

## Description of Download Artifacts

The following table describes the download artifacts included in SnappyData 1.3.0 release:

| Artifact Name | Description |
| ------------- | ----------- |
|snappydata-1.3.0-bin.tar.gz | Full product binary (includes Hadoop 3.2.0) |
|snappydata-jdbc\_2.11-1.3.0.jar | JDBC client driver and push down JDBC data source for Spark |
|snappydata-core\_2.11-1.3.0.jar | The single jar needed in Smart Connector mode; an alternative to --packages option |
|snappydata-odbc\_1.3.0_win.zip | 32-bit and 64-bit ODBC client drivers for Windows |
|snappydata-1.3.0.sha256 | The SHA256 checksums of the product artifacts. On Linux verify using `sha256sum --check snappydata-1.3.0.sha256`. |
|snappydata-1.3.0.sha256.gpg | GnuPG signature for snappydata-1.3.0.sha256. Get the public key using `gpg --keyserver hkps://keys.gnupg.net --recv-keys 573D42FDD455480DC33B7105F76D50B69DB1586C`. Then verify using `gpg --verify snappydata-1.3.0.sha256.gpg`. |
|[snappydata-zeppelin\_2.11-0.8.2.1.jar](https://github.com/TIBCOSoftware/snappy-zeppelin-interpreter/releases/download/v0.8.2.1/snappydata-zeppelin_2.11-0.8.2.1.jar) | The Zeppelin interpreter jar for SnappyData compatible with Apache Zeppelin 0.8.2. The standard jdbc interpreter is preferred over this. See [How to Use Apache Zeppelin with SnappyData](../howto/use_apache_zeppelin_with_snappydata.md). |
