# Release Notes
The SnappyData team is pleased to announce the availability of version 1.0.2 of the platform. You can find the release artifacts of its Community Edition towards the end of this page.

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

The following new features are included in SnappyData 1.0.2 version:

*	Introduced an API in snappy session catalog to get Primary Key of Row tables  or Key Columns of Column Tables, as DataFrame. 
*	Introduced an API in snappy session catalog to get table type as String.
*	Added support for arbitrary size view definition. It use to fail when view text size went beyond 32k.
Support for displaying VIEWTEXT for views in SYS.HIVETABLES. 
For example: Select viewtext from sys.hivetables where tablename = ‘view_name” will give the text with which the view was created.
*	Added Row level Security feature. Admins can define multiple security policies on tables for different users or LDAP groups. Refer [Row Level Security](/security/row_level_security.md)
*	Auto refresh of UI page. Now the SnappyData UI page gets updated automatically and frequently. Users need not refresh or reload. Refer [SnappyData Pulse](/monitoring/monitoring.md)
*	Richer user interface. Added graphs for memory, CPU consumption etc. for last 15 minutes. The user has the ability to see how the cluster health has been for the last 15 minutes instead of just current state.
*	Total CPU core count capacity of the cluster is now displayed on the UI. </br>Refer [SnappyData Pulse](/monitoring/monitoring.md)
*	Bucket count of tables are also displayed now on the user interface.
*	Support deployment of packages and jars as DDL command. Refer [Deploy](/reference/sql_reference/deploy.md)
*	Added support for reading maven dependencies using **--packages** option in our job server scripts. Refer [Deploying Packages in SnappyData](/connectors/deployment_dependency_jar.md#deploypackages).
*	Changes to procedure **sys.repair_catalog** to execute it on the server (earlier this was run on lead by sending a message to it). This will be useful to repair catalog even when lead is down. </br>Refer [Catalog Repair](/troubleshooting/catalog_inconsistency.md)
*	Added support for **PreparedStatement.getMetadata() JDBC API**. This is on an experimental basis.
*	Added support for execution of some DDL commands viz CREATE/DROP DISKSTORE, GRANT, REVOKE. CALL procedures from snappy session as well.
*	Quote table names in all store DDL/DML/query strings to allow for special characters  and keywords in table names.
*	Spark application with same name cannot be submitted to SnappyData. This has been done so that individual apps can be killed by its name when required.
*	Users are not allowed to create tables in their own schema based on system property - `snappydata.RESTRICT_TABLE_CREATION`. In some cases it may be required to control use of cluster resources in which case the table creation is done only by authorized owners of schema.
*	Schema can be owned by an LDAP group also and not necessarily by a single user.
*	Support for deploying SnappyData on Kubernetes using Helm charts. </br>Refer [Kubernetes](/kubernetes.md)
*	Disk Store Validate tool enhancement. Validation of disk store can find out all the inconsistencies at once.
*	BINARY data type is same as BLOB data type.

## Performance Enhancements

The following performance enhancements are included in SnappyData 1.0.2 version:

*	Fixed concurrent query performance issue by resolving the incorrect output partition choice.  Due to numBucket check, all the partition pruned queries were converted to hash partition with one partition. This was causing an exchange node to be introduced. (SNAP-2421)
*	Fixed SnappyData UI becoming unresponsive on LowMemoryException.(SNAP-2071)
*	Cleaning up tokenization handling and fixes. Main change is addition of the following two separate classes for tokenization: 

	*	**ParamLiteral**
	*	**TokenLiteral** 

	Both classes extend a common trait **TokenizedLiteral**. Tokenization will always happen independently of plan caching, unless it is explicitly turned  off. (SNAP-1932)

*	Procedure for smart connector iteration and fixes. Includes fixes for perf issues as noted for all iterators (disk iterator, smart connector and remote iterator). (SNAP-2243)


## Select Fixes and Performance Related Fixes

The following defect fixes are included in SnappyData 1.0.2 version:

*	Fixed incorrect server status shown on the UI. Sometimes due to a race condition for the same member two entries were shown up on the UI. (SNAP-2433)
*	Fixed missing SQL tab on SnappyData UI in local mode. (SNAP-2470)
*	Fixed few issues related to wrong results for Row tables due to plan caching. (SNAP-2463 - Incorrect pushing down of OR and AND clause filter combination in push down query, SNAP-2351 - re-evaluation of filter was not happening due to plan caching, SNAP-2451, SNAP-2457)
*	Skip batch, if the stats row is missing while scanning column values from disk. This was already handled for in-memory batches and the same has been added for on-disk batches. (SNAP-2364)
*	Fixes in UI to forbid unauthorized users to view any tab. (ENT-21)
*	Fixes in SnappyData parser to create inlined table. (SNAP-2302), ‘()’ as optional in some function like ‘current_date()’, ‘current_timestamp()’ etc. (SNAP-2303)
*	Consider the current schema name also as part of Caching Key for plan caching. So same query on same table but from different schema should not clash with each other. (SNAP-2438)
*	Fix for COLUMN table shown as ROW table on dashboard after LME in data  server. (SNAP-2382)
*	Fixed off-heap size for Partitioned Regions, showed on UI. (SNAP-2186)
*	Fixed failure when query on view does not fallback to Spark plan in case Code Generation fails. (SNAP-2363)
*	Fix invalid decompress call on stats row.(SNAP-2348). Use to fail in run time while scanning column tables.(SNAP-2348)
*	Fixed negative bucket size with eviction. (GITHUB-982)
*	Fixed the issue of incorrect LowMemoryException, even if a lot of memory was left. (SNAP-2356)
*	Handled int overflow case in memory accounting. Due to this ExecutionMemoryPool released more memory than it has throws AssertionError (SNAP-2312)
*	Fixed the pooled connection not being returned to the pool after authorization check failure which led to unusable cluster. (SNAP-2255)
*	Fixed different results of nearly identical queries, due to join order. Its due to EXCHANGE hash ordering being different from table partitioning. It will happen for the specific case when query join order is different from partitioning of one of the tables while the other table being joined is partitioned differently. (SNAP-2225)
*	Corrected row count updated/inserted in a column table via putInto. (SNAP-2220)
*	Fixed the OOM issue due to hive queries. This was a memory leak. Due to this the system became very slow after sometime even if idle. (SNAP-2248) 
*	Fixed the issue of incomplete plan and query string info in UI due to plan caching changes.
*	Corrected the logic of existence join.
*	Sensitive information, like user password, LDAP password etc, which are passed as properties to the cluster are masked on the UI now.
*	Schema with boolean columns sometimes returned incorrect null values. Fixed. (SNAP-2436)
*	Fixed the scenario where break in colocation chain of buckets due to crash led to disk store metadata going bad causing restart failure.
*	Wrong entry count on restart, if region got closed on a server due to DiskAccessException leading to a feeling of loss of data. Do not let the region close in case of LME. This has been done by not letting non IOException get wrapped in DiskAccessException. (SNAP-2375)
*	Fix to avoid hang or delay in stop when **stop** is issued and the component has gone into reconnect cycle. (SNAP-2380)
*	Handle joining of new servers better. Avoid ConflictingPersistentDataException when a new server starts before any of the old server start. SNAP-2236
*	ODBC driver bug fix. Added **EmbedDatabaseMetaData.getTableSchemas**.
*	Change the order in which backup is taken. Internal DD diskstore of backup is taken first followed by rest of the disk stores. This helps in stream apps which want to store offset of replayable source in snappydata. They can create the offset table backed up by the internal DD store instead of default or custom disk store.


## Description of Download Artifacts

The following table describes the download artifacts included in SnappyData 1.0.2 version:

| Artifact Name | Description | 
| ------------- |:-------------:| 
|snappydata-1.0.2-bin.tar.gz| Full product binary (includes Hadoop 2.7) |
|snappydata-1.0.2-without-hadoop-bin.tar.gz| Product without the Hadoop dependency JARs |
|snappydata-client-1.6.2.jar|Client (JDBC) JAR|
|[snappydata-zeppelin_2.11-0.7.3.2.jar](https://github.com/SnappyDataInc/zeppelin-interpreter/releases/download/v0.7.3.2/snappydata-zeppelin_2.11-0.7.3.2.jar)| The Zeppelin interpreter jar for SnappyData, compatible with Apache Zeppelin 0.7.3 |
|[snappydata-ec2-0.8.2.tar.gz](https://github.com/SnappyDataInc/snappy-cloud-tools/releases/download/v0.8.2/snappydata-ec2-0.8.2.tar.gz)|Script to Launch SnappyData cluster on AWS EC2 instances


