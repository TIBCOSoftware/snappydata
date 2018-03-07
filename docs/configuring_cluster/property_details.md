# Properties Details

## autoBroadcastJoinThreshold

**Description**</br>
Configures the maximum size in bytes for a table that is broadcast to all server nodes when performing a join.  By setting this value to **-1** broadcasting can be disabled.

**Default Values**</br>
10L * 1024 * 1024 

**Components**</br>
Can be set using a `SET SQL` command or using the configuration properties in the *conf/leads* file. The `SET SQL` command sets the property for the current SnappySession while setting it in *conf/leads* file sets the property for all SnappySession.

**Example**</br>

``` 
// To set auto broadcast
snc.sql(s"set spark.sql.autoBroadcastJoinThreshold=<_SizeInBytes_>")
```

``` 
// To disable auto broadcast
.set("spark.sql.autoBroadcastJoinThreshold", "-1")
```

## bind-address

**Description**</br>
IP address on which the locator is bound. The default behavior is to bind to all local addresses.

**Default Values**</br>

localhost

**Components**</br>
- Server
- Lead
- Locator

**Example**</br>
```
-client-bind-address=localhost
```

## classpath

**Description**</br>
Location of user classes required by the SnappyData Server. </br> This path is appended to the current classpath.

**Default Values**</br>

**Components**</br>
- Server
- Lead
- Locator

**Example**</br>

## critical-heap-percentage

**Description**</br>
Sets the Resource Manager's critical heap threshold in percentage of the old generation heap, 0-100. 
If you set `heap-size`, the default value for `critical-heap-percentage` is set to 90% of the heap size. </br>
Use this switch to override the default.</br>
When this limit is breached, the system starts canceling memory-intensive queries, throws low memory exceptions for new SQL statements, and so forth, to avoid running out of memory.

**Default Values**</br>

**Components**</br>
- Server
- Lead

**Example**</br>
```
conf.set("snappydata.store.critical-heap-percentage", "95")
```

## critical-off-heap-percentage

**Description**</br>
Sets the critical threshold for off-heap memory usage in percentage, 0-100.</br>
When this limit is breached, the system starts canceling memory-intensive queries, throws low memory exceptions for new SQL statements, and so forth, to avoid running out of off-heap memory.

**Default Values**</br>

**Components**</br>
- Server

**Example**</br>
```
conf.set("snappydata.store.critical-off-heap-percentage", "95")
```


## dir

**Description**</br>
Working directory of the server that contains the SnappyData Server status file and the default location for the log file, persistent files, data dictionary, and so forth (defaults to the current directory).

**Default Values**</br>

**Components**</br>
- Server
- Lead
- Locator

**Example**</br>
```
localhost -log-file=/home/user1/snappy/server/snappy-server.log
```

## eviction-heap-percentage

**Description**</br>
Sets the memory usage percentage threshold (0-100) that the Resource Manager will use to start evicting data from the heap. By default, the eviction threshold is 81% of whatever is set for  `critical-heap-percentage`.</br>
Use this switch to override the default.

**Default Values**</br>

**Components**</br>
- Server
- Lead

**Example**</br>

## eviction-off-heap-percentage

**Description**</br>

Sets the off-heap memory usage percentage threshold, 0-100, that the Resource Manager uses to start evicting data from off-heap memory.</br>
By default, the eviction threshold is 81% of whatever is set for `critical-off-heap-percentage`. </br>
Use this switch to override the default.

**Default Values**</br>

**Components**</br>
- Server

**Example**</br>

## heap-size

**Description**</br>
Sets the maximum heap size for the Java VM, using SnappyData default resource manager settings. </br>For example, -heap-size=1024m. </br>If you use the `-heap-size` option, by default SnappyData sets the critical-heap-percentage to 90% of the heap size, and the `eviction-heap-percentage` to 81% of the `critical-heap-percentage`. </br>SnappyData also sets resource management properties for eviction and garbage collection if they are supported by the JVM.

**Default Values**</br>

**Components**</br>
- Server
- Lead
- Locator

**Example**</br>

## J
**Description**</br>
JVM option passed to the spawned SnappyData server JVM. </br>For example, use -J-Xmx1024m to set the JVM heap to 1GB.

**Default Values**</br>

**Components**</br>
- Server
- Lead
- Locator

**Example**</br>

## J-Dgemfirexd.hostname-for-clients

**Description**</br>
Hostname or IP address that is sent to clients so they can connect to the locator. The default is the `bind-address` of the locator.

**Default Values**</br>

**Components**</br>
- Server
- Lead
- Locator

**Example**</br>

## locators

**Description**</br>
List of locators as comma-separated host:port values used to communicate with running locators in the system and thus discover other peers of the distributed system. </br>The list must include all locators in use and must be configured consistently for every member of the distributed system.

**Default Values**</br>

**Components**</br>
- Server
- Lead
- Locator

**Example**</br>

## log-file

**Description**</br>
Path of the file to which this member writes log messages (default is snappyserver.log in the working directory)

**Default Values**</br>

**Components**</br>
- Server
- Lead
- Locator

**Example**</br>

## member-timeout

**Description**</br>
Uses the member-timeout server configuration, specified in milliseconds, to detect the abnormal termination of members. The configuration setting is used in two ways:</br> 1) First, it is used during the UDP heartbeat detection process. When a member detects that a heartbeat datagram is missing from the member that it is monitoring after the time interval of 2 * the value of member-timeout, the detecting member attempts to form a TCP/IP stream-socket connection with the monitored member as described in the next case.</br> 2) The property is then used again during the TCP/IP stream-socket connection. If the suspected process does not respond to the are you alive datagram within the time period specified in member-timeout, the membership coordinator sends out a new membership view that notes the member's failure. </br>Valid values are in the range 1000..600000.

**Default Values**</br>

**Components**</br>
- Server
- Lead
- Locator

**Example**</br>

## memory-size

**Description**</br>
Specifies the total memory that can be used by the node for column storage and execution in off-heap. The default value is 0 (OFF_HEAP is not used by default)

**Default Values**</br>

**Components**</br>
- Server
- Lead

**Example**</br>

## peer-discovery-address

**Description**</br>
Use this as value for the port in the "host:port" value of `locators` property.

**Default Values**</br>

**Components**</br>
- Locator

**Example**</br>

## peer-discovery-port

**Description**</br>

Port on which the locator listens for peer discovery (includes servers as well as other locators).  </br>Valid values are in the range 1-65535, with a default of 10334.

**Default Values**</br>

**Components**</br>
- Locator

**Example**</br>

## rebalance

**Description**</br>
Triggers a rebalancing operation for all partitioned tables in the system. </br>The system always tries to satisfy the redundancy of all partitioned tables on new member startup regardless of this option.
**Default Values**</br>

**Components**</br>
- Server

**Example**</br>

## snappydata.column.batchSize

**Description**</br>
The default size of blocks to use for storage in the SnappyData column store (in bytes or k/m/g suffixes for the unit). The default value is 24M.
**Default Values**</br>

**Components**</br>
- Lead

**Example**</br>

## spark.driver.maxResultSize

**Description**</br>
Limit of the total size of serialized results of all partitions for each action (e.g. collect). The value should be at least 1M or 0 for unlimited. Jobs will be aborted if the total size of results is above this limit. Having a high limit may cause out-of-memory errors in the lead.

**Default Values**</br>

**Components**</br>
- Lead

**Example**</br>

## spark.executor.cores

**Description**</br>
The number of cores to use on each server.

**Default Values**</br>

**Components**</br>
- Lead

**Example**</br>


## spark.local.dir

**Description**</br>
Directory to use for "scratch" space in SnappyData, including map output files and RDDs that get stored on disk. This should be on a fast, local disk in your system. It can also be a comma-separated list of multiple directories on different disks.

**Default Values**</br>

**Components**</br>
- Lead

**Example**</br>

## spark.network.timeout

**Description**</br>
The default timeout for all network interactions while running queries.

**Default Values**</br>

**Components**</br>
- Lead

**Example**</br>

## -thrift-ssl-properties

**Description**</br>
Comma-separated SSL properties including:</br>`protocol`: default "TLS",</br>`enabled-protocols`: enabled protocols separated by ":"</br>`cipher-suites`: enabled cipher suites separated by ":"</br>`client-auth`=(true or false): if client also needs to be authenticated </br>`keystore`: Path to key store file </br>`keystore-type`: The type of key-store (default "JKS") </br>`keystore-password`: Password for the key store file</br>`keymanager-type`: The type of key manager factory </br>`truststore`: Path to trust store file</br>`truststore-type`: The type of trust-store (default "JKS")</br>`truststore-password`: Password for the trust store file </br>`trustmanager-type`: The type of trust manager factory </br> 

**Default Values**</br>

**Components**</br>
- Server

**Example**</br>

## Behavior

**Description**</br>
The action to be taken if the error computed goes outside the error tolerance limit. The default value is`DO_NOTHING`. </br>This property can be set as connection property in the Snappy SQL shell.
Synopsis Data Engine has HAC support using the following behavior clauses:
- `do_nothing`: The SDE engine returns the estimate as is.
- `local_omit`: For aggregates that do not satisfy the error criteria, the value is replaced by a special value like "null".
- `strict`: If any of the aggregate column in any of the rows do not meet the HAC requirement, the system throws an exception.
- `run_on_full_table`: If any of the single output row exceeds the specified error, then the full query is re-executed on the base table.
- `partial_run_on_base_table`: If the error is more than what is specified in the query, for any of the output rows (that is sub-groups for a group by query), the query is re-executed on the base table for those sub-groups. This result is then merged (without any duplicates) with the result derived from the sample table.

SDE Property
**Default Values**</br>

**Components**</br>
- Lead

**Example**</br>

## ColumnBatchSize

**Description**</br>
The default size of blocks to use for storage in SnappyData column and store. When inserting data into the column storage this is the unit (in bytes or k/m/g suffixes for unit) that is used to split the data into chunks for efficient storage and retrieval. </br> This property can also be set for each table in the `create table` DDL.

**Default Values**</br>

**Components**</br>
Can be set using a `SET SQL` command or using the configuration properties in the *conf/leads* file. The `SET SQL` command sets the property for the current SnappySession while setting it in *conf/leads* file sets the property for all SnappySession.
**Example**</br>


## ColumnMaxDeltaRows

**Description**</br>
The maximum number of rows that can be in the delta buffer of a column table. The size of delta buffer is already limited by `ColumnBatchSize` property, but this allows a lower limit on the number of rows for better scan performance. So the delta buffer is rolled into the column store whichever of `ColumnBatchSize` and this property is hit first. It can also be set for each table in the `create table` DDL, else this setting is used for the `create table
**Default Values**</br>

**Components**</br>
Can be set using a `SET SQL` command or using the configuration properties in the *conf/leads* file. The `SET SQL` command sets the property for the current SnappySession while setting it in *conf/leads* file sets the property for all SnappySession.
**Example**</br>


## Confidence

**Description**</br>
Confidence with which the error bounds are calculated for the approximate value. It should be a fractional value not exceeding 1. </br> The default value is0.95. </br>x

**Default Values**</br>

**Components**</br>
Confidence with which the error bounds are calculated for the approximate value. It should be a fractional value not exceeding 1. </br> The default value is0.95. </br>This property can be set as connection property in the Snappy SQL shell.

**Example**</br>

## EnableExperimentalFeatures


**Description**</br>

**Default Values**</br>

**Components**</br>

**Example**</br>


## Error

**Description**</br>
Maximum relative error tolerable in the approximate value calculation. It should be a fractional value not exceeding 1. The default value is0.2. </br>

The following four methods are available to be used in query projection when running approximate queries:

- **absolute_error(column alias)**: Indicates absolute error present in the estimate (approx answer) calculated using error estimation method (ClosedForm or Bootstrap)
- **relative_error(column alias)**: Indicates ratio of absolute error to estimate.
- **lower_bound(column alias)**: Lower value of an estimate interval for a given confidence.
- **upper_bound(column alias)**: Upper value of an estimate interval for a given confidence.

SDE Property

**Default Values**</br>

**Components**</br>
This property can be set as connection property in the Snappy SQL shell

**Example**</br>

## FlushReservoirThreshold


**Description**</br>
Reservoirs of sample table will be flushed and stored in columnar format if sampling is done on the base table of size more than flushReservoirThreshold. The default value is10,000.</br> 
SDE Property

**Default Values**</br>

**Components**</br>
This property must be set in the *conf/servers* and *conf/leads* file

**Example**</br>


## ForceLinkPartitionsToBuckets

**Description**</br>

**Default Values**</br>

**Components**</br>

**Example**</br>


## HashAggregateSize

**Description**</br>
Aggregation uses optimized hash aggregation plan but one that does not overflow to disk and can cause OOME if the result of aggregation is large. The limit specifies the input data size (in bytes or k/m/g/t suffixes for unit) and not the output size. Set this only if there are queries that can return large number of rows in aggregation results. The default value is set to 0 which means, no limit is set on the size, so the optimized hash aggregation is always used.
**Default Values**</br>

**Components**</br>
Can be set using a `SET SQL` command or using the configuration properties in the *conf/leads* file. The `SET SQL` command sets the property for the current SnappySession while setting it in *conf/leads* file sets the property for all SnappySession.

**Example**</br>


## HashJoinSize

**Description**</br>
The join would be converted into a hash join if the table is of size less than the `hashJoinSize`.  The limit specifies an estimate on the input data size (in bytes or k/m/g/t suffixes for unit). The default value is 100MB.
**Default Values**</br>

**Components**</br>
Can be set using a `SET SQL` command or using the configuration properties in the *conf/leads* file. The `SET SQL` command sets the property for the current SnappySession while setting it in *conf/leads* file sets the property for all SnappySession.

**Example**</br>

## JobServerEnabled


**Description**</br>

**Default Values**</br>

**Components**</br>

**Example**</br>


## JobServerWaitForInit

**Description**</br>

**Default Values**</br>

**Components**</br>

**Example**</br>


## NumBootStrapTrials

**Description**</br>
Number of bootstrap trials to do for calculating error bounds. The default value is100. </br>
SDE Property

**Default Values**</br>

**Components**</br>

This property must be set in the *conf/leads* file.
**Example**</br>


## ParserTraceError

**Description**</br>

**Default Values**</br>

**Components**</br>

**Example**</br>


## PartitionPruning

**Description**</br>

**Default Values**</br>

**Components**</br>

**Example**</br>


## PlanCaching

**Description**</br>

**Default Values**</br>

**Components**</br>

**Example**</br>


## PlanCachingAll

**Description**</br>
Number of query plans that will be cached.

**Default Values**</br>

**Components**</br>
Can be set using a `SET SQL` command or using the configuration properties in the *conf/leads* file. The `SET SQL` command sets the property for the current SnappySession while setting it in *conf/leads* file sets the property for all SnappySession.

**Example**</br>

## PlanCacheSize

**Description**</br>
Number of query plans that will be cached.

**Default Values**</br>


**Components**</br>

**Example**</br>
## PreferPrimariesInQuery

**Description**</br>

**Default Values**</br>

**Components**</br>

**Example**</br>


## SchedulerPool

**Description**</br>

**Default Values**</br>

**Components**</br>

**Example**</br>


## SnappyConnection

**Description**</br>

**Default Values**</br>

**Components**</br>

**Example**</br>


## Tokenize

**Description**</br>

**Default Values**</br>

**Components**</br>

**Example**</br>



