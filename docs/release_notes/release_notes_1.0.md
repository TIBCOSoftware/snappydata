# Release Notes 

The SnappyData team is pleased to announce the availability of SnappyData Release 1.0.0.

## New Features/Fixed Issues

  [SNAP-953] Add RPM/DEB installer packaging targets using the Netflix Nebula ospackage gradle
  plugin.

  [SNAP-2039] Correct null updates to column tables. (#861)

  Use concurrent TrieMaps in SnappySession contextObjects, and queryHints map. Reason being that
  SnappySession can be read concurrently by multiple threads from same query for sub-query/broadcast
  kind of plans where planning for the BroadcastExchangeExec plan happens in parallel on another
  thread.

  [SNAP-2029] Added new "snappydata.preferPrimaries" option to prefer primaries for queries. (#852)
  Avoid double memory at the cost of reduced scalability but still having a hot backup.
  See discussion on Slack: https://snappydata-public.slack.com/archives/C0DCF0UGG/p1505460492000378

  Fixed a parser issue where AS can be optional in namedExpression rule. This fixes Q77 of TPCDS.

  [SNAP-2030] Now routed update and delete query on row table would return number of affected rows.

  [SNAP-2028] Snappy Python APIs fixes. (#851)
  A) Some of the SparkSession python APIs used to pass SQLContext to DataFrameWriter and
  DataFrameReader APIs.
  B) Fixed truncate table API.

  Fixed a couple of issues in parser. (#849)
  1. Order by and sort by clauses after partition by can be optional.
  2. INTERVAL non reserved key word was being treated as an identifier because of optional clauses
  ordering.

  [SNAP-2022] Remove the check which tested if any lead is already stopped, in snappy-stop-all.sh
  (#845). This was causing the script to skip shutting down of other running leads, if any. Added
   a check for rowstore, so that 'sbin/snappy-stop-all.sh rowstore' doesn't see the message.

  [SNAP-2020] Track in-progress insert size to avoid data skew. (#844)
  With many concurrent inserts/partitions on a node, significant data skew in inserts was still
  observed (on machines with large number of cores like 32) due to same smallest bucket being
  chosen by multiple partitions. This change now tracks the in-progress size for bucket and adds
  that to determine smallest bucket.

  [SNAP-2012] Skip locked entries in evictor. (#839)
  Fix as suggested by @rishitesh to use Unsafe API to try acquire monitor on RegionEntry.

  Hiding commands not applicable to snappydata (will be continued to be displayed for GemFireXD and
  RowStore mode). (#838)

  [SNAP-2003] Fix for 'stream to big table join CQ returning incorrect result'. (#829)
  HashJoinExec's streamPlan and buildPlan RDDs are computed on each CQ execution.

  [AQP-293] Changes for JNI UTF8String.contains. (#832)
  Convert UTF8Strings in ParamLiteral to off-heap when snappydata's off-heap is enabled.
  Changes in SnappyParser. Also, updated parboiled2 to latest release.

  [SNAP-1995] Added a python example showcasing KMeans usage. (#827)

  Fix an issue in collect-debug-artifacts script with extraction. Skip any configuration checks in
  collect-debug-artifacts for extraction (-x, --extract=).

  [SNAP-1993] Fixes for data skew when no partition_by is provided. (#825)
  With these changes, distribution in ColumnCacheBenchmark test, for example, is nearly equal most
  of the time among the buckets. Other cases like those reported originally with 7M rows have only
  ~50% difference between min and max (as compared to ~4X originally)

  Remove ParamLiteral for LIKE/RLIKE/REGEXP. If expression foldable is false, then LIKE family
  generates very suboptimal plan (if not converted to Contains/StartsWith/EndsWith) that will
  compile the Regex for every row.

  [SNAP-1984] Changes to retain UnifiedMemoryManager state across executor restart by copying the
  state in a temporary memory manager, which is created when store boots up but Spark environment is
  not ready. (#821)

  [SNAP-1981] For prepare phase, avoid rules that do not handle NullType since that is what is used
  as placeholder for params. (#815)

  [SNAP-1851] Properly closing the connection in case when connection commit fails. (#796)

  [SNAP-1976] Changes to set isolation level. (#813)
  Allow operations on row and column tables if isolation level is set to other than NONE and
  autocommit is true (query routing is enabled). If autocommit is false, query routing will be
  disabled and transactions on row tables will be supported. Queries on column tables will error out
  when query routing is disabled.

  [SNAP-1973]/[SNAP-1970] Avoid clearing hive meta-store system properties. (#816)
  The hive meta-store system properties are required to be set for static initialization of Hive and
  should not be cleared because a concurrent hive context initialization (from some other path) can
  see inconsistencies like system property found but not available when actually read.

  [SNAP-1979] Added MemoryManagerStats for capturing different stats for UnifiedMemoryManager.(#814)
  Smart Cconnector mode will not have these stats as GemFireXD cache will not be available.

  [SNAP-1982] Change batch UUID to be a long (#812)
  Now using region.newUUID to generate the batch UUID. Use colocatedRegion of column table (the row
  buffer) to generate the UUID since that is what smart connector and internal rollover uses.

  [SNAP-1611] Increased spark.memory.fraction from 92% to 97% (#808)
  We want to give a little buffer to JVM before it reaches the critical hep size.

  Make SnappySession.contextObjects as transient to fix the serialization issues reported on
  spark-shell when SnappySession gets captured in closures (e.g. import session.implicits._ with
  toDF)

  [SNAP-1955] Fixes for issues seen in parallel test runs (#805)

  [SNAP-1660] Remove password from product logging.

  [SNAP_1948] Added an option to specify streaming batch interval during streaming job submission.
  e.g. bin/snappy-job.sh submit --lead localhost:8090 --app-name appname --class appclass \
      --app-jar appjar --conf logFileName=demo.txt --stream --batch-interval 4000

  [SNAP-1893] Changed locator status to RUNNING after stopped locator is restarted with
  snappy-start-all.sh

  [SNAP-1877] GC issues with large dictionaries in decoding and other optimizations (#787)
  1. Performance issues with dictionary decoder when dictionary is large. 2. Data skew fixes. 3.
  Using a consistent sort order so that generated code is identical across sessions for the same
  query. 4. Reducing the size of generated code.

  Fix issues seen during concurrency testing (#782)

  [SNAP-1884] Fixed result mismatch in join between snappy table and temp table.

  Overridden two methods from Executor.scala. (#783) These methods have been added in Spark
  executor to check store related errors.

  [SNAP-1917] Properly comparing datatype of complex schema.

  [SNAP-1919]/[SNAP-1916] Added isPartitioned flag to determine partitioned tables (#784)

  [SNAP-1904] Use same connection for rowbuffer and columnstore.

  [SNAP-1883] Parser change for range operator.

  Fixed: After new job classloader changes executors are not fetching driver files. (#777)

  [SNAP-1894] Codegen issue for query with case in predicate expression (#772)

  [SNAP-1888]/[SNAP-1886] Fixed parser error in two level nested subQuery, works with Spark (#774)

  [Snap 1833] Fixed the synchronization problem with sc.addJar() (#728)

  [SNAP-1377]/[SNAP-902] Proper handling of exceptions in case of Lead and Server HA  (#758)

  [Snap 1871] Remove custom built-in jdbc provider and instead use spark's JDBC provider (#757)

  [SNAP-1882] Changes done for routing update and delete queries on column table to lead node.
  Also handled prepared statement on update and delete queries for column table.

  [SNAP-1885] Fixed Semijoin returning incorrect result (#768)

  [SNAP-1787] - Handling Array[Decimal] in both embedded and split mode (#754)

  [SNAP-1892] .show() after table creation using CreateExternalTable api gives empty/null
  entries, caused due to empty UserSpecifiedSchema instead of None (#764)

  [SNAP-1734] Query plan shows 0 number of output rows at the start of the plan. (#761)
  Snappy's execution happens in two phases. First phase the plan is executed to create a rdd
  which is then used to create a CachedDataFrame. In second phase, the CachedDataFrame is then
  used for further actions. For accumulating the metrics for first phase,
  SparkListenerSQLPlanExecutionStart is fired. This keeps the current executionID in
  _executionIdToData but does not add it to the active executions. This ensures that query is not
  shown in the UI but the new jobs that are run while the plan is being executed are tracked
  against this executionID. In the second phase, when the query is actually executed,
  SparkListenerSQLPlanExecutionStart adds the execution data to the active executions.
  SparkListenerSQLPlanExecutionEnd is then sent with the accumulated time of both the phases. For
  consuming SparkListenerSQLPlanExecutionStart, Snappy's SQLListener has been added. Overridden
  withNewExecutionId in CachedDataFrame so that the above explained process also happens when the
  dataset APIs are used.

  [SNAP-1878] Proper handling of path option while creation of external table using API (#760)

  [SNAP-1850] Remove connection used in JDBCSourceAsColumnarStore#getPartitionID v2 (#750)

  [SNAP-1389] Update and delete support for column tables (#747)

  [SNAP-1426] Fixed the Snappy Dashboard freezing issue when loading data sets (#732)

  Making background start of multi-node cluster as default

  [SNAP-1860] Close the connection if \commit/rollback is not done (#746)
  Made changes to make sure to commit/rollback the snapshot tx in case of exception. e.g Security
  related while trying to iterate over the region.

  [SNAP-1656] Security support in snappydata (#731)
  Enable LDAP based authentication and authorization in Snappydata cluster.

  Support for snapshot transactional insert in column table (#718)

  [SNAP-1825]/[SNAP-1818] DDL routing changes  (#742)
  Fix for ALTER TABLE ADD column does not work in case of row table when the table is altered
  after inserting data and CREATE ASYNCEVENTLISTENER doesn't work with lead node.

  Removing old 2.0.x backward compatibility classes.

  Fixes the "describe table" from Spark and shows the full schema.

  [SNAP-1268] Code changes to start SnappyTableStatsProviderService service only once. (#738)

  [SNAP-1838] skip plan cache clear if there is no SparkContext

  Fixes for issues found during concurrency testing (#730)

  [SNAP-1815] Disallow configuration of Hive metsatore using hive.metastore.uris property in
  hive-site.xml (#714)

  [SNAP-1708] collect-debug-artifacts script won't need both way ssh now. (#723)

  [SNAP-1723] When foldable functions are there in the queries and literals are there in their
  argument then identify case where Tokenization should be stopped. Added a bunch of such functions
  with corresponding relevant argument numbers for that. (#706)

  [SNAP-1806] Changed the exception handling in SnappyConnector mode. (#719)

  Support for setting scheduler pools using the set command (#700)

  [SNAP-671] Added support for DSID to work for column tables (#716)

  Added a task context listener to explicitly remove the obtained memory. (#713)

  [SNAP-1326] SnappyParser changes to support ALTER TABLE ADD/DROP COLUMN DDLs (#711)

  [SNAP-1808] Create cachedbatch tables in user's schema instead of the earlier common schema
  SNAPPYSYS_INTERNAL. Changes from Sumedh @sumwale (#712)

  [SNAP-1805] Fixed Query Execution statistics are not getting displayed in SQL graph, caused
  because function to withNewExecutionId was executed before it was passed as argument (#703)

  [SNAP-1777] Increasing default member-timeout for SnappyData (#704)

  [SNAP-1610] Removing the code related to split cluster mode (that was disabled for users in 0.9
  release) (#696)

  [SNAP-1363] Performance degrades because of PoolExhaustedException when run from connector mode.
  Increasing max connection pool size since there is an idle timeout in the pool implementations
  (default: 120s), so cleanup of unused connections will happen in any case.

  [SNAP-1794] Modified code generation of DynamicFoldableExpression such that even the
  initMutableState splits into multiple init() functions, code will be generated properly. (#699)

  Changes for Apache Spark 2.1.1 merge (#695)

  [SNAP-1451] set default startup-recovery-delay to 102s for Snappy tables to avoid interfering
  with initial bucket creation.

  [SNAP-1722] Test to validate support for long, short, tinyint and byte datatypes for row tables
  (#689)

  Spark 2.1 Merge (#501)

  Fixing NoSuchElementException "None get" in dropTable. Using the global SparkContext directly
  instead of getting from active SparkSession (which may not exist) in hive meta-store listener.

  [SNAP-1688] CachedDataFrame memory allocation should be accounted with execution memory rather
  than storage memory.

  [SNAP-1748] Fixed: Without persistence, data loading is unsuccessful with eviction on (#682)

  [SNAP-1721] Avoid code generation failure in WorkingWithObject.scala example (#685)

  Changes for SNAP-1678 Smart connector should emit info logs that indicate the cluster to which it is connecting (#676)

  [SNAP-1760] Correct null bitset expansion and reduce copying in inserts. (#678) Fixes
  ArrayIndexOutOfBounds exception in queries with wide schema having nulls.

  Corrected the scaladoc examples in SnappySession. (#672)

  Allow for spaces at start of API parser calls

  [SNAP-1737] While passing value to GemFireXD, it should ve converted from catalyst type to scala
  type.(#669)

  [SNAP-1735] use single batch count in stats row (#664)

  Renamed "-b" option to "-bg" to match convention used in other POSIX commands

  [SNAP-1725] Fix start and collect-debug scripts for Mac.

  [SNAP-1714] Correcting case-sensitivity handling for API calls (#657)

  [SNAP-1792] Snappy Monitoring UI now also displays Member Details View which shows member specific
  information, various statistics (like Status, CPU, Memory, Heap & Off-Heap Usages, etc) and
  members logs in incremental way.
  
  [Snap-1890] Snappy Monitoring UI displays new Pulse logo. Also product and it's build details are
  shown under version in pop up window.
  
  [Snap-1813] Pulse (Snappy Monitoring UI) users need to provide valid user name and password if
  SnappyData cluster is running in secure mode.
