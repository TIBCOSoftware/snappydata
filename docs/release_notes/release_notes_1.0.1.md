# Release Notes 
The SnappyData team is pleased to announce the availability of version 1.0.1 of the platform.
[**Download the Enterprise Edition here**](https://www.snappydata.io/download).

## New Features

- putInto and deleteFrom bulk operations support for column tables (SNAP-2092, SNAP-2093, SNAP-2094):

  - Ability to specify "key columns" in the table DDL to use for putInto and deleteFrom APIs

  - "PUT INTO" SQL or putInto API extension to overwrite existing rows and insert non-existing ones

  - "DELETE FROM" SQL or deleteFrom API extension to delete a set of matching rows

  - UPDATE SQL now supports using expressions with column references of another table in RHS of SET

- Improvements in cluster restart with off-line, failed nodes or with corrupt meta-data (SNAP-2096)

  - New admin command "unblock" to allow the initialization of a table even if it is waiting for offline members

  - No discard of data (unlike revoke) with the new approach and initialize with the latest online working copy
    (SNAP-2143)

  - Parallel recovery of data regions to break any cyclic dependencies between the nodes, and allow reporting
    on all off-line nodes that may have more recent copy of data

  - Many bug-fixes related to startup issues due to meta-data inconsistencies:

    - Incorrect ConflictingPersistentDataExeption at restart due to disk-store meta-data corruption
      (SNAP-2097, SNAP-2098)

    - Metadata corruption issues causing GII to fail (SNAP-2140)

- Compression of column batches in disk storage and over the network (SNAP-1743)

  - Support for LZ4 and SNAPPY compression codecs in persistent storage and transport for column table data

  - Smart connector uses compression when pulling from remote hosts while using uncompressed
    for same host for best performance

  - New SOURCEPATH and COMPRESSION columns in SYS.HIVETABLES virtual table
- Support for temporary, global temporary and persistent VIEWs (SNAP-2072):

  - CREATE VIEW, CREATE TEMPORARY VIEW and CREATE GLOBAL TEMPORARY VIEW DDLs

  - SQL from JDBC/ODBC for VIEWs routed to Spark execution engine

- External connectors (like cassandra) used from smart connector no longer require any jars in the
  snappydata cluster (SNAP-2072)

- External tables display in dashboard and snappy command-line (SNAP-2086)

- Auto-configuration of SPARK_PUBLIC_DNS, hostname-for-clients etc in AWS environment (SNAP-2116)

- Out-of-the-box support for AWS URLs/APIs in the product

- GRANT/REVOKE SQL support in SnappySession.sql() earlier only allowed from JDBC/ODBC (SNAP-2042)

- LATERAL VIEW support in SnappySession.sql() (SNAP-1283)

- FETCH FIRST syntax as an alternative to LIMIT to support some SQL tools that use former

- Addition of IndexStats in for local row table index lookup and range scans

- SYS.DISKSTOREIDS virtual table to disk-store IDs being used in the cluster by all members (SNAP-2113)

- Show ARRAY/MAP/STRUCT complex types in JDBC/ODBC metadata as part of SNAP-2141

## Performance Enhancements

- Major performance improvements in smart connector mode (SNAP-2101, SNAP-2084)

  - Minimized buffer copying especially when connector executors are colocated with store data nodes

  - Intelligent connector query handling to use key lookups into column table rather than full scan
    for cases of heavily filtered scan queries

  - Reduced round-trips for operations (transactions, bucket pruning)

  - Allow using SnappyUnifiedMemoryManager with smart connector (SNAP-2084)

- New memory and disk iterator to minimize faultins and do serial disk reads across concurrent
  iterators (SNAP-2102):

  - New iterator to substantially reduce faultins in case all data is not in memory

  - Cross-iterator serial disk reads per diskstore to minimize random reads from disk

  - New remote iterator that substantially reduces the memory overhead and caches only current batch

- Startup performance improvements to cut down on locator/server/lead start and restart times (SNAP-338)

- Improve performance of reads of variable length data for some queries (SNAP-2118)

- Use colocated joins with VIEWs when possible (SNAP-2204)

- Separate disk store for delta buffer regions  to substantially improve column table compaction (SNAP-2121)

- Projection push-down to scan layer for non-deterministic expressions like spark_partition_id() (SNAP-2036)

- Parser performance improvements (by ~50%)

- code-generation cache is larger by default and configurable (SNAP-2120)

## Select bug fixes and performance related fixes

A sample of bug fixes done as part of this release are noted below. For a more comprehensive list, see [ReleaseNotes.txt](https://github.com/SnappyDataInc/snappydata/blob/master/ReleaseNotes.txt).

- Now only overflow-to-disk is allowed as eviction action for tables (SNAP-1501):

  - Only overflow-to-disk is allowed as a valid eviction action and cannot be explicitly specified
    (LOCAL_DESTROY removed due to possible data inconsistencies)

  - OVERFLOW=false property can be used to disable eviction which is true by default

- Memory accounting fixes:

  - Incorrect initial memory accounting causing insert failure even with memory available (SNAP-2084)

  - Zero usage shown in UI on restart (SNAP-2180)

- Disable embedded Zeppelin interpreter in a secure cluster which can bypass security (SNAP-2191)

- JSON conversion for complex types ARRAY/STRUCT/MAP (SNAP-2056)

- Fix import of JSON data (SNAP-2087)

- CREATE TABLE ... AS SELECT * fails without explicit schema (SNAP-2047)

- Selects missing results or failing during node failures (SNAP-889, SNAP-1547)

- Incorrect results with joins/anti-joins for some cases having more than one match for a key (SNAP-2212)

- Fixes and improvements to server and lead status in both the launcher status and SYS.MEMBERS table
  (SNAP-1960, SNAP-2060, SNAP-1645)

- Fix global temporary tables/views with SnappySession (SNAP-2072)

- Fix updates on complex types (SNAP-2141)

- Column table scan fixes related to null value reads (SNAP-2088)

- Incorrect reads of column table statistics rows in some cases (SNAP-2124)

- Disable tokenization for external tables and session flag to disable it and plan caching
  (SNAP-2114, SNAP-2124)

- Table meta-data issues with squirrel client and otherwise (SNAP-2083)

- Case-sensitive column names now work correctly with all operations (GITHUB-900 and other related fixes)

- Allow for hyphens in schema names

- Deadlock in transactional operations with GII (SNAP-1950)

- Couple of fixes in UPDATE SQL:

  - Failure due to rollover during update operation (SNAP-2192)

  - Subquery updates shown as ResultSet rather than update count (SNAP-2156)

- Correct mismatch between executor and DistributedMember names in some configurations that caused
  Remote routing of partitions (SNAP-2122)

- Fixes ported from Apache Geode (GEODE-2109, GEODE-2240)

- Fixes to all failures in snappy-spark test suite which includes both product and test changes

- Fixes related to ODBC driver calls to thrift server

- More comprehensive python API testing (SNAP-2044)