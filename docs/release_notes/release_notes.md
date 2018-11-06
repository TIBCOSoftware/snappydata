# Release Notes
The SnappyData team is pleased to announce the availability of version 1.0.2.1 of the platform. You can find the release artifacts of its Community Edition towards the end of this page.

You can also download the Enterprise Edition [here](https://www.snappydata.io/download). The following table summarizes the features available in Enterprise and OSS (Community) editions.

| Feature | Community | Enterprise|
| ------------- |:-------------:| :-----:|
|Mutable Row & Column Store| X | X |
|Compatibility with Spark     | X | X |
| Shared Nothing Persistence and HA | X | X |
| REST API for Spark Job Submission | X | X |
| Fault Tolerance for Driver | X | X |
| Access to the system using JDBC Driver | X | X |
| CLI for backup, restore, and export data | X | X |
| Spark console extensions | X | X |
| System Perf/Behavior statistics | X | X |
| Support for transactions in Row tables | X | X |
| Support for indexing in Row Tables | X | X |
| SQL extensions for stream processing | X | X |
| Runtime deployment of packages and jars | X  | X |
| Synopsis Data Engine for Approximate Querying |  | X |
| ODBC Driver with High Concurrency |  | X |
| Off-heap data storage for column tables |  | X |
| CDC Stream receiver for SQL Server into SnappyData |  | X |
| GemFire/Apache Geode connector |  | X |
|Row Level Security|  | X |
| Use encrypted password instead of clear text password |  | X |
| Restrict Table, View, Function creation even in user’s own schema|  | X |
| LDAP security interface |  | X |

## New Features 

The following new features are included in SnappyData 1.0.2.1 version:

*	Support Spark's HiveServer2 in SnappyData cluster. Enables starting an embedded Spark HiveServer2 on leads in embedded mode.
*	Provided a default Structured Streaming Sink implementation for SnappyData column and row tables. Conflation of events with same key columns can be enabled by a sink property.
*	Added a -agent jvm argument in the launch commands to kill the jvm as soon as OOM occurs. This is important because the VM sometimes used to crash in unexpected ways later as a side effect of this corrupting internal metadata which later gave restart troubles.
*	Allow NONE as a valid policy for server-auth-provider. Essentially, the cluster can now be configured only for user authentication and mutual peer to peer authentication of cluster members can be disabled by specifying this property as NONE.
*	Add support for query hints to force a join type. This may be useful for cases where the result is known to be small, for example, but plan rules cannot determine so.
*	Allow deleteFrom API to work as far as the dataframe contains key columns.

## Performance Enhancements

The following performance enhancements are included in SnappyData 1.0.2.1 version:

*	Avoid shuffle when join key columns are a superset of child partitioning. 
*	Added a pooled version of SnappyData JDBC driver for Spark to connect to SnappyData cluster as JDBC datasource.
*	[SNAP-2657] Added caching for hive catalog lookups. Meta-data queries with large number of tables take quite long because of nested loop joins between  SYSTABLES and HIVETABLES for most meta-data queries. Even if the table numbers were in hundreds it used to take a lot of time.


## Select Fixes and Performance Related Fixes

The following defect fixes are included in SnappyData 1.0.2.1 version:

*	[SNAP-2659] Reset the pool at the end of collect to avoid spill over of low latency pool setting to latter operations that may not use the CachedDataFrame execution paths.
*	[SNAP-2491] Fixed: Column added using 'ALTER TABLE ... ADD COLUMN ...' through SnappyData shell does not reflect in spark-shell. 
*	Fixing occasional failures in serialization using CachedDataFrame, if the node is just starting/stopping. Also, fix a hang in shutdown for cases where hive client close is trying to boot up the node again, waiting on the locks taken during the shutdown.
*	[SNAP-2566] Lead and Lag window functions were failing due to incorrect analysis error.
Fixed the validate-disk-store tool. It was not getting initialized with registered types. This was required to deserialize byte arrays being read from persisted files.
*	Fix schema in ResultSet metadata. It used to show the default schema 'APP' always.
*	[SNAP-2627] Sometimes a false unique constraint violation happened due to removed or destroyed AbstractRegionEntry. Now an attempt is made to remove it from index and another try is made to put the new value against the index key.
*	[SNAP-2654] Fix for memory leak in oldEntrieMap leading to LowMemoryException and OutOfMemoryException.


## Description of Download Artifacts

The following table describes the download artifacts included in SnappyData 1.0.2.1 version:

| Artifact Name | Description | 
| ------------- |:-------------:| 
|snappydata-1.0.2.1-bin.tar.gz| Full product binary (includes Hadoop 2.7) |
|snappydata-1.0.2.1-without-hadoop-bin.tar.gz| Product without the Hadoop dependency JARs |
|snappydata-jdbc_2.11-1.0.2.1.jar|Client (JDBC) JAR|
|[snappydata-zeppelin_2.11-0.7.3.4.jar](https://github.com/SnappyDataInc/zeppelin-interpreter/releases/download/v0.7.3.4/snappydata-zeppelin_2.11-0.7.3.4.jar)| The Zeppelin interpreter jar for SnappyData, compatible with Apache Zeppelin 0.7.3 |
|[snappydata-ec2-0.8.2.tar.gz](https://github.com/SnappyDataInc/snappy-cloud-tools/releases/download/v0.8.2/snappydata-ec2-0.8.2.tar.gz)|Script to Launch SnappyData cluster on AWS EC2 instances


