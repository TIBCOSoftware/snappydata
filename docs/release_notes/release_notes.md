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
|GemFire connector |  |


## New Features

SnappyData 1.3.0 release includes the following new features over the previous 1.2.0 release:

* **Open sourcing of non-OSS components**</br>

    All components except for the streaming GemFire connector are now OSS! This includes the Approximate Querying
    Engine, off-heap storage for column tables, the streaming JDBC connector for CDC, security modules and the ODBC driver.
    All new OSS modules are available under the Apache Source License v2.0 like the rest of the product.
    Overall the new 1.3.0 OSS release is both more feature rich than the erstwhile 1.2.0 Enterprise product,
    and more efficient.</br></br>

* **Online compaction of column block data**</br>

    Automatic online compaction of column blocks that have seen significant percentage of deletes or updates.
    The compaction is triggered in one of the foreground threads performing delete or update operations
    to avoid the "hidden" background operational costs of the platform. The ratio of data at which compaction
    is triggered can be configured using two cluster level (or system) properties:</br>

    * _snappydata.column.compactionRatio_: for the ratio of deletes to trigger compaction (default 0.1)
    * _snappydata.column.updateCompactionRatio_: for the ratio of updates to trigger compaction (default 0.2)</br></br>

* **Transparent disk overflow of large query results**</br>

    Queries that return large results have been a problem with Spark and SnappyData alike due to lack
    of streaming of the final results to application layer resulting in large heap consumption on the driver.
    There are properties like _spark.driver.maxResultSize_ in Spark to altogether disallow large query results.
    SnappyData has had driver-side persistence of large results for JDBC/ODBC to reduce the memory pressure
    but even so fetching multi-GB results was not possible in most cases and would lead to OOME on driver or
    the server that is servicing the JDBC/ODBC client.</br>

    This new feature adds disk overflow for large query results on the executors completely eliminating all
    memory problems for any size of results. It works when using the JDBC/ODBC driver and for the
    SnappySession.sql().toLocalIterator() API. Note that usage of any other Dataset APIs will result in
    creation of base Spark Dataset implementation that will not use disk overflow. A couple of
    cluster level (or system) properties can be used to fine-tune the behaviour:</br>

    * _spark.sql.maxMemoryResultSize_: maximum size of results from a JDBC/ODBC/SQL query in a partition
      that will be held in memory beyond which the results will be written to disk, while the maximum
      size of a single disk block is fixed to 8 times this value (default 4mb)
    * _spark.sql.resultPersistenceTimeout_: maximum duration in seconds for which results overflowed to disk
      are held on disk after which they are cleaned up (default 21600 i.e. 6 hours)</br></br>

* **Eager cleanup of broadcast join data blocks**</br>

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
    has not exceeded the execution cache limit.</br>

    This new feature eagerly removes broadcast join data blocks at the end of query from the executors
    (which can still fetch from driver on demand) and also from the driver for DML executions.</br></br>

* **Hadoop upgraded to version 3.2.0 and added ADLS gen 1/2 connectivity**</br>

    Hadoop upgraded to 3.2.0 from 2.7.x to allow support for newer components like ADLS gen 1/2.
    ADLS gen 2 jars added to the product by default to enable support for Azure Data Lake Storage (ADLS) out of the box.</br></br>

* **Enable LRUCOUNT eviction for column tables to enable creation of disk-only column tables**</br>

    LRUCOUNT based eviction was disabled for column tables since the count would represent column blocks
    per data node while the number of rows being cached in memory would be indeterminate. This is now
    enabled to allow for cases like disk-only tables with minimal memory caching. So now one can create
    a column table like:

    ```
        CREATE TABLE diskTable (...) using column options (eviction_by 'lrucount 1')
    ```

    This will keep only one column block in memory which will be few MB at maximum (in practise only
    few KB since statistics blocks will be preferred for caching). Documentation covers the fact that
    LRUCOUNT based eviction for column tables will lead to indeterminate memory usage.</br></br>

* **Spark layer updated to v2.1.3**</br>

    SnappyData Smart Connector will continue to support Apache Spark 2.1.1 as well as later 2.1.x releases.</br></br>

* **JDBC driver now supports Java 11**</br>

    The JDBC driver is now compatible with Java 11 and higher till Java 16. It will continue to be
    compatible with Java 8 like the rest of the product.

Apart from the above new features, the interactive execution of Scala code using **EXEC SCALA** SQL
that was marked experimental in the previous release is now considered production ready.

<a id="odbc-changes"></a>
### ODBC Driver

Apart from open sourcing of the source code, the SnappyData ODBC Driver has also seen many enhancements
over the previous TIBCO ComputeDB™ ODBC Driver 1.2.0 release:

* **Transparent reconnect to the cluster after failure**</br>

    If a connection to the cluster fails for some reason (network or anything else), then the driver
    will attempt to reconnect to the cluster on the next operation, including trying failover on all the
    available locators and servers of the cluster. This is enabled by setting the new `AutoReconnect`
    option to true (default is false).</br></br>

* **Ability to read passwords securely from the system credential manager**</br>

    The new boolean `CredentialManager` option can be enabled to allow reading the login password as well
    as private SSL key password (for SSL mutual authentication) from the system credential manager.
    On Windows this uses the standard CredRead() API with type as CRED_TYPE_GENERIC so user must add
    the user name and password under Generic Windows Credentials, then provide the address key in the password fields.
    On Mac OSX the "security" tool is used to look up the password for the given value as provided in the password field.
    On Linux and other UNIXes, the `secret-tool` utility must be installed (`libsecret-tools` package on debian/ubuntu
    based systems, `libsecret` on most of the others) which is used to look up the password with the
    password field split on the first ':' to obtain the attribute and its value.</br></br>

* **Set the default schema to use on the connection**</br>

    The default schema on a connection is normally set to the same name as the user. The new `DefaultSchema` option
    can be used to change it to a different value which is equivalent to the `USE <SCHEMA>` SQL statement.</br></br>

* **New API implementations for SQLCancel and SQLCancelHandle and fixes to many existing ones**</br>

    The SQLCancel and SQLCancelHandle APIs are now implemented. A number of existing APIs including SQLPutData,
    SQLBindParameter, SQLGetDiagRec, SQLGetDiagField have seen bug fixes while SQLGetInfo has been enhanced.</br></br>

* **Updated build dependencies Thrift, Boost, OpenSSL to new releases having many fixes**


## Stability and Performance Improvements

SnappyData 1.3.0 includes the following stability and performance improvements:

* Bulk send for multiple statement close messages in hive-metastore getAllTables to substantially
  improve metastore performance.

* Optimize putInto for small puts using a local cache. (PR#1563)

* Increase default putInto join cache size to infinite and allow it to overflow to disk if large.

* Switch to safe ThreadUtils await methods throughout the code and merge fixes for
  [SPARK-13747](https://issues.apache.org/jira/browse/SPARK-13747).
  This fixes potential ThreadLocal leaks in RPC when using ForkJoinPool.

* Cleanup to use a common SnapshotConnectionListener to handle snapshot transactions fixing
  cases of snapshots ending prematurely especially with the new column compaction feature.

* Add caching of resolved relations in SnappySessionCatalog. This gives a large boost to Spark external
  table queries when the table metadata is large (for example, file based tables having millions of files).

* Added boolean _snappydata.sql.useDriverCollectForGroupBy_ property to allow driver do the direct collect
  of partial results for top-level GROUP BY. This avoids the last EXCHANGE for partial results of a GROUP BY
  query improving the performance substantially for sub-second queries. It should be enabled only when the
  final size of results of the query is known to be small else can cause heap memory issues on the driver.

* Dashboard statistics collection now cleanly handles cases where the collection takes more time than
  its refresh interval (default 5 seconds). Previously it could cause multiple threads to pile up and
  miss updating results from a delayed collection thread.

* Added a pool for UnsafeProjections created in ColumnDelta.mergeStats to reduce the overhead of generated
  code creation and compilation in every update.

* For column table update/delete, make the output partitioning of ColumnTableScan to be ordered on bucketId.
  This avoids unnecessary local sort operators being introduced in the update/delete/put plans.

* Continue for all SparkListener failures except OutOfMemoryException. This allows the product to continue
  working for cases where custom SparkListener supplied by user throws an exception.

* Reduce overheads of EventTracker and TombstoneService to substantially decrease heap usage
  in applications doing continuous update/delete/put operations.

* Merged pull request #2 from crabo/spark-multiversion-support to improve OldEtriesCleanThread and hot sync locks.

* Removed fsync for DataDictionary that causes major performance issues for large hive metastore updates
  that also uses the DataDictionary. Replication of DataDictionary across all data nodes and locators
  ensures its resilience.

* Merged patches for the following Spark issues for increased stability of the product:
    - [SPARK-27065](https://issues.apache.org/jira/browse/SPARK-27065): Avoid more than one active task set managers
      for a stage (causes termination of DAGScheduler and thus the whole JVM).
    - [SPARK-13747](https://issues.apache.org/jira/browse/SPARK-13747): Fix potential ThreadLocal leaks in RPC when
      using ForkJoinPool (can cause applications using ForkJoinPool to fail e.g. for scala's Await.ready/result).
    - [SPARK-26352](https://issues.apache.org/jira/browse/SPARK-26352): Join reorder should not change the order of
      output attributes (can cause unexpected query failures or even JVM SEGV failures).
    - [SPARK-25081](https://issues.apache.org/jira/browse/SPARK-25081): Nested spill in ShuffleExternalSorter
      should not access released memory page (can cause JVM SEGV and related failures).
    - [SPARK-21907](https://issues.apache.org/jira/browse/SPARK-21907): NullPointerException in
      UnsafeExternalSorter.spill() due to OutOfMemory during spill.


## Resolved Issues

SnappyData 1.3.0 resolves the following major issues:

* [SPARK-31918](https://issues.apache.org/jira/browse/SPARK-31918):
  SparkR support for R 4.0.0+

* [SNAP-3306](https://jirasnappydataio.atlassian.net/browse/SNAP-3306):
  Row tables with altered schema having added columns causes failure in recovery mode:</br>
  Allowing -1 as value for SchemaColumnId, for the case when the column is missing in the schema-version
  under consideration. This happens when it tries to read older rows, which doesn't have the new column. (PR#1529)

* [SNAP-3326](https://jirasnappydataio.atlassian.net/browse/SNAP-3326):
  Allow option of replacing the StringType to desired SQL type (VARCHAR, CHAR, CLOB etc)
  when creating table by importing external table. The option `string_type_as = [VARCHAR|CHAR|CLOB]`
  now allows for switching to the required SQL type.

* [SDENT-175](https://jirasnappydataio.atlassian.net/browse/SDENT-175):
  Support for cancelling query via JDBC (PR#1539)

* [SDENT-171](https://jirasnappydataio.atlassian.net/browse/SDENT-171):
  rand() alias in sub-select is not working. Fixed collectProjectsAndFilters for nondeterministic
  functions in PR#1541.

* [SDENT-187](https://jirasnappydataio.atlassian.net/browse/SDENT-187):
  Wrong results returned by queries against partitioned ROW table.
  Fix by handling EqualTo(Attribute, Attribute) case (PR#1544).
  Second set of cases fixed by handling comparisons other than EqualTo. (PR#1546)

* [SDENT-199](https://jirasnappydataio.atlassian.net/browse/SDENT-199):
  Fix for select query with where clause showing the plan without metrics in SQL UI tab. (PR#1552)

* [SDENT-170](https://jirasnappydataio.atlassian.net/browse/SDENT-170):
  Return JDBC metadata in lower case. (Store PR#550)

* [SDENT-185](https://jirasnappydataio.atlassian.net/browse/SDENT-185):
  If connections to ComputeDB is opened, using both hostname and IP address,
  the ComputeDB ODBC driver throws a System.Data.Odbc.OdbcException.

* [SDENT-139](https://jirasnappydataio.atlassian.net/browse/SDENT-139):
  Exception in extracting SQLState (when it's not set or improper) masks original problem.

* [SDENT-195](https://jirasnappydataio.atlassian.net/browse/SDENT-195):
  Fix NPE by adding a null check for Statement instance before getting its maxRows value.(Store PR#559)

* [SDENT-194](https://jirasnappydataio.atlassian.net/browse/SDENT-194):
  Return the table type as just a `table` instead of `ROW TABLE` or `COLUMN TABLE`.(Store PR#560)

* [GITHUB-1559](https://github.com/TIBCOSoftware/snappydata/issues/1559):
  JDBC ResultSet metadata search is not case-insensitive.
  Add upper-case names to search index of ClientResultSet. (PR#565)

* [SNAP-3332](https://jirasnappydataio.atlassian.net/browse/SNAP-3332):
  PreparedStatement: Fix for precision/scale of DECIMAL parameter types not sent in
  execute causing them to be overwritten by another PreparedStatement. (PR#1562, Store PR#567)

* Fix for occasional putInto/update test failures. (Store PR#568)

* Native C++ driver (used by the ODBC driver) updated to the latest versions of library dependencies
  (thrift, boost, openssl) with many fixes.

* Fixes to the scripts for bind-address and hostname-for-clients auto setup, `hostname -I` not available
  on some systems and others.

* Switch eclipse-collections to fastutil due to being more comprehensive and better performance.

* [SNAP-3145](https://jirasnappydataio.atlassian.net/browse/SNAP-3145):
  Test coverage for external hive metastore.

* [SNAP-3321](https://jirasnappydataio.atlassian.net/browse/SNAP-3321):
  Example for EXEC SCALA via JDBC.

* Expanded CDC and Data Extractor test coverage.


## Known Issues

The following among the known issues have been **fixed** over the previous 1.2.0 release:

| Key | Item | Description |
| --- | ---- | ----------- |
|[<s>SNAP-3306</s>](https://jirasnappydataio.atlassian.net/browse/SNAP-3306) [FIXED] | Row tables with altered schema having added columns causes failure in recovery mode. | A new column that is added in an existing table in normal mode fails to get restored in the recovery mode. |

For the remaining known issues, see the **Known Issues** section of [1.2.0 release notes](https://raw.githubusercontent.com/TIBCOSoftware/snappydata/community_docv1.2.0/docs/release_notes/TIB_compute-ce_1.2.0_relnotes.pdf#%5B%7B%22num%22%3A21%2C%22gen%22%3A0%7D%2C%7B%22name%22%3A%22XYZ%22%7D%2C69%2C720%2C0%5D).

## Description of Download Artifacts

The following table describes the download artifacts included in SnappyData 1.3.0 release:

| Artifact Name | Description |
| ------------- | ----------- |
|snappydata-1.3.0-bin.tar.gz     | Full product binary (includes Hadoop 3.2.0). |
|snappydata-jdbc\_2.11-1.3.0.jar | JDBC client driver and push down JDBC data source for Spark. Compatible with Java 8, Java 11 and higher. |
|snappydata-core\_2.11-1.3.0.jar | The single jar needed in Smart Connector mode; an alternative to --packages option. Compatible with Spark versions 2.1.1, 2.1.2 and 2.1.3. |
|snappydata-odbc\_1.3.0\_win64.zip | 32-bit and 64-bit ODBC client drivers for Windows 64-bit platform. |
|[snappydata-zeppelin\_2.11-0.8.2.1.jar](https://github.com/TIBCOSoftware/snappy-zeppelin-interpreter/releases/download/v0.8.2.1/snappydata-zeppelin_2.11-0.8.2.1.jar) | The Zeppelin interpreter jar for SnappyData compatible with Apache Zeppelin 0.8.2. The standard jdbc interpreter is now recommended instead of this. See [How to Use Apache Zeppelin with SnappyData](../howto/use_apache_zeppelin_with_snappydata.md). |
|snappydata-1.3.0.sha256 | The SHA256 checksums of the product artifacts. On Linux verify using `sha256sum --check snappydata-1.3.0.sha256`. |
|snappydata-1.3.0.sha256.asc | PGP signature for snappydata-1.3.0.sha256 in ASCII format. Get the public key using `gpg --keyserver hkps://keyserver.ubuntu.com --recv-keys A7994CE77A24E5511A68727D8CED09EB8184C4D6`. Then verify using `gpg --verify snappydata-1.3.0.sha256.asc` which should show the mentioned key in the verification with email as `build@snappydata.io`. |
