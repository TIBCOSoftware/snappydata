# Overview

SnappyData™ is a memory-optimized database based on Apache Spark. It delivers very high
throughput, low latency, and high concurrency for unified analytic workloads that may combine
streaming, interactive analytics, and artificial intelligence in a single, easy to manage distributed cluster.

Prior to release 1.3.0 there were two editions namely, the Community Edition which was a fully
functional core OSS distribution that was under the Apache Source License v2.0, and the
Enterprise Edition which was sold by TIBCO Software under the name TIBCO ComputeDB™ that included
everything offered in the OSS version along with additional capabilities that are closed source
and only available as part of a licensed subscription.

The SnappyData team is pleased to announce the availability of version 1.3.1 of the platform.
Starting with release 1.3.0, all the platform's private modules have been made open-source apart from
the streaming GemFire connector (which depends on non-OSS Pivotal GemFire product jars).
These include [Approximate Query Processing (AQP)](https://github.com/TIBCOSoftware/snappy-aqp)
and the [JDBC connector](https://github.com/TIBCOSoftware/snappy-connectors) repositories
which also include the off-heap storage support for column tables and the security modules.
In addition, the [ODBC driver](https://github.com/TIBCOSoftware/snappy-odbc) has also been
made open-source. With this, the entire code base of the platform (apart from the GemFire
connector) has been made open source and there is no longer an Enterprise edition distributed by TIBCO.

You can find details of the release artifacts towards the end of this page.

The full set of documentation for SnappyData 1.3.1 including installation guide, user guide
and reference guide can be found [here](https://tibcosoftware.github.io/snappydata/1.3.1/).

Release notes for the previous 1.3.0 release can be found
[here](https://tibcosoftware.github.io/snappydata/1.3.0/release_notes/release_notes).

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

SnappyData 1.3.1 release includes the following new features over the previous 1.3.1 release:

* **Support for Log4J 2.x (2.17.2) which is used by default**<br/>

    Following up with the exposure of Log4Shell and related vulnerabilities, SnappyData Platform has moved
    to the latest Log4J 2.x (2.17.2) from the previous Log4J 1.x. Patches were ported for the Spark components
    (where support for Log4J 2.x will land only with the 3.3.0 release), while other components were updated
    to use Log4J/SLF4J. The Spark connector component supports both Log4J 2.x and Log4J 1.x to allow compatibility
    with upstream Spark releases while the SnappyData's Spark distribution only uses Log4J 2.x.


## Stability and Security Improvements

SnappyData 1.3.1 includes the following changes to improve stability and security of the platform:

* Use timed Process.waitFor instead of loop on exitValue() which is unreliable.

* Fixed a race condition in old entries cleaner thread deleting in-use snapshot entries.

* Allow retry in startup failure even for data nodes. In some rare cases region initialization may fail
  due to colocated region still being initialized, so retry region initialization for such cases.

* Fixed UDF name lookups to do exact regex match in the CSV list in the meta-data region.

* Apart from Log4J, following dependencies were updated to address known security issues:
    - Jetty upgraded to 9.4.44.v20210927
    - Eclipse Jersey upgraded to 2.35
    - jackson-mapper-asl and jackson-core-asl upgraded to 1.9.14-atlassian-6
    - jackson and jackson-databind upgraded to 2.13.3
    - Kafka upgraded to 2.2.2
    - Protobuf upgraded to 3.16.1
    - Ant upgraded to 1.10.12
    - Apache HttpClient upgraded to 4.5.13
    - Upgrades to Apache commons dependencies:
        - commons-io: 2.6 => 2.11.0
        - commons-codec: 1.11 => 1.15
        - commons-compress: 1.4.1 => 1.21
        - commons-beanutils: 1.9.3 => 1.9.4
    - [SPARK-34110](https://issues.apache.org/jira/browse/SPARK-34110): Upgrade Zookeeper to 3.6.2
    - [SPARK-37901](https://issues.apache.org/jira/browse/SPARK-37901): Upgrade Netty to 4.1.73
    - Netty further upgraded to 4.1.77
    - gcs-hadoop-connector upgraded to hadoop3-2.1.2

* Ported patches for the following issues from Apache Geode:
    - [GEODE-1252](https://issues.apache.org/jira/browse/GEODE-1252): Modify bits field atomically
    - [GEODE-2802](https://issues.apache.org/jira/browse/GEODE-2802): Tombstone version vector to contain only
    the members that generate the tombstone
    - [GEODE-5278](https://issues.apache.org/jira/browse/GEODE-5278): Unexpected CommitConflictException caused by
    faulty region synchronization
    - [GEODE-4083](https://issues.apache.org/jira/browse/GEODE-4083): Fix infinite loop caused
      by thread race changing version
    - [GEODE-3796](https://issues.apache.org/jira/browse/GEODE-3796): Changes are made to
      validate region version after the region is initialized
    - [GEODE-6058](https://issues.apache.org/jira/browse/GEODE-6058): recordVersion should
      allow update higher local version if for non-persistent region
    - [GEODE-6013](https://issues.apache.org/jira/browse/GEODE-6013): Use expected initial
      image requester's rvv information
    - [GEODE-2159](https://issues.apache.org/jira/browse/GEODE-2159): Add serialVersionUIDs to
      exception classes not having them
    - [GEODE-5559](https://issues.apache.org/jira/browse/GEODE-5559): Improve runtime of
      RegionVersionHolder.canonicalExceptions
    - [GEODE-5612](https://issues.apache.org/jira/browse/GEODE-5612):
      Fix RVVExceptionB.writeReceived()
    - [GEODE-7085](https://issues.apache.org/jira/browse/GEODE-7085): Ensure that the bitset
      stays within BIT_SET_WIDTH and is flushed in all code paths
    - GFE-50415: Wait for membership change in persistence advisor can hang if the member
      join event was missed
    - [GEODE-5111](https://issues.apache.org/jira/browse/GEODE-5111): Set offline members to
      null only when done waiting for them

* Merged patches for the following Spark issues:
    - [SPARK-6305](https://issues.apache.org/jira/browse/SPARK-6305): Migrate from log4j1 to log4j2
    - Followup [SPARK-37684](https://issues.apache.org/jira/browse/SPARK-37684) and
      [SPARK-37774](https://issues.apache.org/jira/browse/SPARK-37774) to upgrade log4j to 2.17.x
    - [SPARK-37791](https://issues.apache.org/jira/browse/SPARK-37791): Use log4j2 in examples
    - [SPARK-37794](https://issues.apache.org/jira/browse/SPARK-37794): Remove internal log4j
      bridge api usage
    - [SPARK-37746](https://issues.apache.org/jira/browse/SPARK-37746):
      log4j2-defaults.properties is not working since log4j 2 is always initialized by default
    - [SPARK-37792](https://issues.apache.org/jira/browse/SPARK-37792): Fix the check of
      custom configuration in SparkShellLoggingFilter
    - [SPARK-37795](https://issues.apache.org/jira/browse/SPARK-37795): Add a scalastyle rule
      to ban `org.apache.log4j` imports
    - [SPARK-37805](https://issues.apache.org/jira/browse/SPARK-37805):
      Refactor `TestUtils#configTestLog4j` method to use log4j2 api
    - [SPARK-37889](https://issues.apache.org/jira/browse/SPARK-37889): Replace Log4j2
      MarkerFilter with RegexFilter
    - [SPARK-26267](https://issues.apache.org/jira/browse/SPARK-26267): Retry when detecting
      incorrect offsets from Kafka
    - [SPARK-37729](https://issues.apache.org/jira/browse/SPARK-37729):
      Fix SparkSession.setLogLevel that is not working in Spark Shell
    - [SPARK-37887](https://issues.apache.org/jira/browse/SPARK-37887): Fix the check of REPL
      log level
    - [SPARK-37790](https://issues.apache.org/jira/browse/SPARK-37790): Upgrade SLF4J to 1.7.32
    - [SPARK-22324](https://issues.apache.org/jira/browse/SPARK-22324): Upgrade Arrow to 0.8.0
    - [SPARK-25598](https://issues.apache.org/jira/browse/SPARK-25598): Remove flume connector
      in Spark
    - [SPARK-37693](https://issues.apache.org/jira/browse/SPARK-37693):
      Fix ChildProcAppHandleSuite failed in Jenkins maven test


## Resolved Issues

SnappyData 1.3.1 resolves the following major issues apart from the patches noted in the previous section:

* [SDSNAP-825](https://jira.tibco.com/browse/SDSNAP-825): Update can leave a dangling snapshot lock on the table


## Known Issues

The known issues noted in [1.3.0 release notes](https://tibcosoftware.github.io/snappydata/1.3.0/release_notes/release_notes/#known-issues)
still apply in 1.3.1 release. These have been reproduced below for reference:

| Key | Item | Description | Workaround |
| --- | ---- | ----------- | ---------- |
| [SNAP-1422](https://jirasnappydataio.atlassian.net/browse/SNAP-1422) | Catalog in Smart connector inconsistent with servers. | Catalog in Smart connector is inconsistent with servers when a table is queried from spark-shell (or from an application that uses Smart connector mode) the table metadata is cached on the Smart connector side. If this table is dropped from the SnappyData Embedded cluster (by using snappy-shell, or JDBC application, or a Snappy job), the metadata on the Smart connector side stays cached even though the catalog has changed (table is dropped). In such cases, the user may see unexpected errors such as `org.apache.spark.sql.AnalysisException: Table "SNAPPYTABLE" already exists` in the Smart connector app side, for example, for DataFrameWriter.saveAsTable() API if the same table name that was dropped is used in saveAsTable(). | <ul><li>User may either create a new SnappySession in such scenarios</li><b>OR</b><li>Invalidate the cache on the Smart Connector mode. For example, by calling `snappy.sessionCatalog.invalidateAll()`.</li></ul> |
| [SNAP-1153](https://jirasnappydataio.atlassian.net/browse/SNAP-1153) | Creating a temporary table with the same name as an existing table in any schema should not be allowed. | When creating a temporary table, the SnappyData catalog is not referred, which means, a temporary table with the same name as that of an existing SnappyData table can be created. Two tables with the same name lead to ambiguity during query execution and can either cause the query to fail or return the wrong results. | Ensure that you create temporary tables with a unique name. |
| [SNAP-2910](https://jirasnappydataio.atlassian.net/browse/SNAP-2910) | DataFrame API behavior in Spark, Snappy. | Saving a Dataset using Spark's JDBC provider with SnappyData JDBC driver into SnappyData row/column tables fails. | Use row or column provider in the Embedded or Smart connector. For Spark versions not supported by Smart connector, use the [SnappyData JDBC Extension Connector](../programming_guide/spark_jdbc_connector.md). |
| [SNAP-3148](https://jirasnappydataio.atlassian.net/browse/SNAP-3148) | Unicode escape character `\u` does not work for `insert into table values()` syntax. | Escape character `\u` is used to indicate that code following `\u` is for a Unicode character but this does not work with `insert into table values ()` syntax that is allowed for column and row tables. | As a workaround, instead of `insert into table values ('\u...', ...)` syntax, use `insert into table select '\u...', ...` syntax. User can also directly insert the Unicode character instead of using an escape sequence.<br/><br/> For example: `create table region (val string, description string) using column`<br/><br/> The following insert query will insert a string value `'\u7ca5'` instead of a Unicode char:<br/><br/> `insert into region values ('\u7ca5', 'unicode2')`<br/><br/> However, following insert statement will insert the appropriate Unicode char:<br/><br/>`insert into region select '\u7ca5', 'unicode2'`<br/><br/> The following query that directly inserts a Unicode char instead of using escape char also works:<br/><br/>`insert into region values ('粤','unicode2')` |
| [SNAP-3146](https://jirasnappydataio.atlassian.net/browse/SNAP-3146) | UDF execution from Smart Connector. | A UDF, once executed from the smart connector side, continues to remain accessible from the same SnappySession on the Smart connector side, even if it is deleted from the embedded side. | Drop the UDF from the Smart connector side or use a new SnappySession. |
| [SNAP-3293](https://jirasnappydataio.atlassian.net/browse/SNAP-3293) | Cache optimized plan for plan caching instead of the physical plan. | Currently, SnappyData caches the physical plan of the query for plan caching. Evaluating the physical plan may lead to an extra sampling job for some type of queries like view creations. Because of this, you may notice an extra job submitted while running the **CREATE VIEW** DDL if the view query contains some operations which require a sampling job. This may impact the performance of the **CREATE VIEW** query. | |
| [SNAP-3298](https://jirasnappydataio.atlassian.net/browse/SNAP-3298) | Credentials set in Hadoop configuration in the Spark context can be set only once without restarting the cluster. | The credentials that are embedded in a FileSystem object. The object is cached in FileSystem cache. The cached object does not get refreshed when there is a configuration (credentials) change. Hence, it uses the initially set credentials even if you have set new credentials. | Run the `org.apache.hadoop.fs.FileSystem.closeAll()` command on `snappy-scala` shell or using `EXEC SCALA` SQL or in a job. This clears the cache. Ensure that there are no queries running on the cluster when you are executing the command. After this you can set the new credentials. |


## Downloading and verifying Release Artifacts

Download all the artifacts listed on the [release](https://github.com/TIBCOSoftware/snappydata/releases/tag/v1.3.1)
page including the externally linked `snappydata-odbc_1.3.0_win64.zip` and
`snappydata-zeppelin_2.11-0.8.2.1.jar` if required, then use the following to verify on Linux or Mac:

```sh
$ gpg --keyserver hkps://keyserver.ubuntu.com --recv-keys A7994CE77A24E5511A68727D8CED09EB8184C4D6
# command below should show a good signature from:
#   SnappyData Inc. (build artifacts) <build@snappydata.io>
$ gpg --verify snappydata-1.3.1.sha256.asc
# command below should show OK for all the artifacts
$ sha256sum --check snappydata-1.3.1.sha256
```

If you do not need some of the artifacts, then you can skip downloading them in which case the `sha256sum` command will show `FAILED open or read` error or equivalent for the missing artifacts. Alternatively you can explicitly run `sha256sum` on only the downloaded artifacts, then manually compare the values to those listed in `snappydata-1.3.1.sha256`.

The following table describes the download artifacts included in SnappyData 1.3.1 release:

| Artifact Name                                                                                                                                                         | Description                                                                                                                                                                                                                                                                                                                                       |
|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [snappydata-1.3.1-bin.tar.gz](https://github.com/TIBCOSoftware/snappydata/releases/download/v1.3.1/snappydata-1.3.1-bin.tar.gz)                                       | Full product binary (includes Hadoop 3.2.0).                                                                                                                                                                                                             |
| [snappydata-jdbc_2.11-1.3.1.jar](https://github.com/TIBCOSoftware/snappydata/releases/download/v1.3.1/snappydata-jdbc_2.11-1.3.1.jar)                                | JDBC client driver and push down JDBC data source for Spark. Compatible with Java 8, Java 11 and higher.                                                                                                                                                 |
| [snappydata-spark-connector_2.11-1.3.1.jar](https://github.com/TIBCOSoftware/snappydata/releases/download/v1.3.1/snappydata-spark-connector_2.11-1.3.1.jar)          | The single jar needed in Smart Connector mode; an alternative to --packages option. Compatible with Spark versions 2.1.1, 2.1.2 and 2.1.3.                                                                                                               |
| [snappydata-odbc_1.3.0_win64.zip](https://github.com/TIBCOSoftware/snappydata/releases/download/v1.3.0/snappydata-odbc_1.3.0_win64.zip)                             | 32-bit and 64-bit ODBC client drivers from 1.3.0 release for Windows 64-bit platform.                                                                                                                                                                    |
| [snappydata-zeppelin_2.11-0.8.2.1.jar](https://github.com/TIBCOSoftware/snappy-zeppelin-interpreter/releases/download/v0.8.2.1/snappydata-zeppelin_2.11-0.8.2.1.jar) | The Zeppelin interpreter jar for SnappyData compatible with Apache Zeppelin 0.8.2. The standard jdbc interpreter is now recommended instead of this. See [How to Use Apache Zeppelin with SnappyData](../howto/use_apache_zeppelin_with_snappydata.md).  |
| [snappydata-1.3.1.sha256](https://github.com/TIBCOSoftware/snappydata/releases/download/v1.3.1/snappydata-1.3.1.sha256)                                               | The SHA256 checksums of the product artifacts.                                                                                                                                                                                                           |
| [snappydata-1.3.1.sha256.asc](https://github.com/TIBCOSoftware/snappydata/releases/download/v1.3.1/snappydata-1.3.1.sha256.asc)                                       | PGP signature for snappydata-1.3.1.sha256 in ASCII format that can be verified using GnuPG or PGP tools.                                                                                                                                                 |
