# List of Properties
|Property|Description|Components</br>|
|-|-|-|
|J|JVM option passed to the spawned SnappyData server JVM. </br>For example, use -J-Xmx1024m to set the JVM heap to 1GB.|Server</br>Lead</br>Locator|
|dir|Working directory of the server that contains the SnappyData Server status file and the default location for the log file, persistent files, data dictionary, and so forth (defaults to the current directory).| Server</br>Lead</br>Locator</br>|
|classpath|Location of user classes required by the SnappyData Server.</br>This path is appended to the current classpath.|Server</br>Lead</br>Locator|
|heap-size|<a id="heap-size"></a> Sets the maximum heap size for the Java VM, using SnappyData default resource manager settings. </br>For example, -heap-size=1024m. </br>If you use the `-heap-size` option, by default SnappyData sets the critical-heap-percentage to 90% of the heap size, and the `eviction-heap-percentage` to 81% of the `critical-heap-percentage`. </br>SnappyData also sets resource management properties for eviction and garbage collection if they are supported by the JVM. |Server</br>Lead</br>Locator|
|memory-size|<a id="memory-size"></a>Specifies the total memory that can be used by the node for column storage and execution in off-heap. The default value is 0 (OFF_HEAP is not used by default)|Server</br>Lead|
|locators|List of locators as comma-separated host:port values used to communicate with running locators in the system and thus discover other peers of the distributed system. </br>The list must include all locators in use and must be configured consistently for every member of the distributed system.|Server</br>Lead</br>Locator|
|<a id="rebalance"></a>rebalance|Triggers a rebalancing operation for all partitioned tables in the system. </br>The system always tries to satisfy the redundancy of all partitioned tables on new member startup regardless of this option.|Server|
|bind-address|IP address on which the locator is bound. The default behavior is to bind to all local addresses.|Server</br>Lead</br>Locator|
|critical-heap-percentage|Sets the Resource Manager's critical heap threshold in percentage of the old generation heap, 0-100. </br>If you set `-heap-size`, the default value for `critical-heap-percentage` is set to 90% of the heap size. </br>Use this switch to override the default.</br>When this limit is breached, the system starts canceling memory-intensive queries, throws low memory exceptions for new SQL statements, and so forth, to avoid running out of memory.|Server</br>Lead|
|eviction-heap-percentage|Sets the memory usage percentage threshold (0-100) that the Resource Manager will use to start evicting data from the heap. By default, the eviction threshold is 81% of whatever is set for `-critical-heap-percentage`.</br>Use this switch to override the default.|Server</br>Lead</br>|
|critical-off-heap-percentage|Sets the critical threshold for off-heap memory usage in percentage, 0-100. </br>When this limit is breached, the system starts canceling memory-intensive queries, throws low memory exceptions for new SQL statements, and so forth, to avoid running out of off-heap memory.|Server|
|eviction-off-heap-percentage|Sets the off-heap memory usage percentage threshold, 0-100, that the Resource Manager uses to start evicting data from off-heap memory. </br>By default, the eviction threshold is 81% of whatever is set for `-critical-off-heap-percentage`. </br>Use this switch to override the default.|Server|
|log-file|Path of the file to which this member writes log messages (default is snappyserver.log in the working directory)|Server</br>Lead</br>Locator|
|J-Dgemfirexd.hostname-for-clients|Hostname or IP address that is sent to clients so they can connect to the locator. The default is the `bind-address` of the locator.|Server|
|peer-discovery-address|Use this as value for the port in the "host:port" value of "-locators" property |Locator|
|peer-discovery-port|Port on which the locator listens for peer discovery (includes servers as well as other locators).  </br>Valid values are in the range 1-65535, with a default of 10334.|Locator|
|member-timeout|Uses the member-timeout server configuration, specified in milliseconds, to detect the abnormal termination of members. The configuration setting is used in two ways:</br> 1) First, it is used during the UDP heartbeat detection process. When a member detects that a heartbeat datagram is missing from the member that it is monitoring after the time interval of 2 * the value of member-timeout, the detecting member attempts to form a TCP/IP stream-socket connection with the monitored member as described in the next case.</br> 2) The property is then used again during the TCP/IP stream-socket connection. If the suspected process does not respond to the are you alive datagram within the time period specified in member-timeout, the membership coordinator sends out a new membership view that notes the member's failure. </br>Valid values are in the range 1000..600000.|Server</br>Locator</br>Lead|
|snappydata.column.batchSize|The default size of blocks to use for storage in the SnappyData column store (in bytes or k/m/g suffixes for the unit). The default value is 24M.|Lead|
|<a id="thrift-properties"></a>thrift-ssl|Specifies if you want to enable or disable SSL. Values: true or false|Server|
|thrift-ssl-properties|Comma-separated SSL properties including:</br>`protocol`: default "TLS",</br>`enabled-protocols`: enabled protocols separated by ":"</br>`cipher-suites`: enabled cipher suites separated by ":"</br>`client-auth`=(true or false): if client also needs to be authenticated </br>`keystore`: Path to key store file </br>`keystore-type`: The type of key-store (default "JKS") </br>`keystore-password`: Password for the key store file</br>`keymanager-type`: The type of key manager factory </br>`truststore`: Path to trust store file</br>`truststore-type`: The type of trust-store (default "JKS")</br>`truststore-password`: Password for the trust store file </br>`trustmanager-type`: The type of trust manager factory </br> |Server|
|spark.driver.maxResultSize|Limit of the total size of serialized results of all partitions for each action (e.g. collect). The value should be at least 1M or 0 for unlimited. Jobs will be aborted if the total size of results is above this limit. Having a high limit may cause out-of-memory errors in the lead.|Lead|
|spark.executor.cores|The number of cores to use on each server. |Lead|
|spark.network.timeout|The default timeout for all network interactions while running queries.|Lead|
|spark.local.dir|Directory to use for "scratch" space in SnappyData, including map output files and RDDs that get stored on disk. This should be on a fast, local disk in your system. It can also be a comma-separated list of multiple directories on different disks.|Lead|

<a id="sql-properties"></a>
## SQL Properties

These properties can be set using a `SET SQL` command or using the configuration properties in the *conf/leads* file. The `SET SQL` command sets the property for the current SnappySession while setting it in *conf/leads* file sets the property for all SnappySession.

For example: Set in the snappy SQL shell

```
snappy> connect client 'localhost:1527';
snappy> set snappydata.column.batchSize=100k;
```
This sets the property for the snappy SQL shell's session.

Set in the *conf/leads* file
```
$ cat conf/leads
# This goes to the default directory 
node-l -heap-size=4096m -spark.ui.port=9090 -locators=node-b:8888,node-a:9999 -spark.executor.cores=10 -snappydata.column.batchSize=100k
```

| Property | Description|
|--------|--------|
|snappydata.column.batchSize |The default size of blocks to use for storage in SnappyData column and store. When inserting data into the column storage this is the unit (in bytes or k/m/g suffixes for unit) that is used to split the data into chunks for efficient storage and retrieval. </br> This property can also be set for each table in the `create table` DDL.|
|snappydata.column.maxDeltaRows|The maximum number of rows that can be in the delta buffer of a column table. The size of delta buffer is already limited by `ColumnBatchSize` property, but this allows a lower limit on the number of rows for better scan performance. So the delta buffer is rolled into the column store whichever of `ColumnBatchSize` and this property is hit first. It can also be set for each table in the `create table` DDL, else this setting is used for the `create table`|
|snappydata.hashJoinSize|The join would be converted into a hash join if the table is of size less than the `hashJoinSize`. The default value is100 MB.|
|snappydata.hashAggregateSize|Aggregation uses optimized hash aggregation plan but one that does not overflow to disk and can cause OOME if the result of aggregation is large. The limit specifies the input data size (with b/k/m/g/t/p suffixes for unit) and not the output size. Set this only if there are queries that can return large number of rows in aggregation results. The default value is set to 0b which means, no limit is set on the size, so the optimized hash aggregation is always used.|
|spark.sql.autoBroadcastJoinThreshold|Configures the maximum size in bytes for a table that is broadcast to all server nodes when performing a join.  By setting this value to **-1** broadcasting can be disabled. |
|`spark.sql.autoBroadcastJoinThreshold`|Configures the maximum size in bytes for a table that is broadcast to all server nodes when performing a join. By setting this value to -1 broadcasting can be disabled. |

<a id="sde-properties"></a>

## SDE Properties

The [SDE](../aqp.md) properties can be set using a Snappy SQL shell (snappy-sql) command or using the configuration properties in the **conf/leads** file. </br>
The command sets the property for the current SnappySession while setting it in **conf/leads** file sets the property for all SnappySession. 

For example: Set in the  Snappy SQL shell (snappy-sql)

```
snappy> connect client 'localhost:1527';
snappy> set snappydata.flushReservoirThreshold=20000;
```
Set in the *conf/leads* file
```
$ cat conf/leads
# This goes to the default directory 
node-l -heap-size=4096m -spark.ui.port=9090 -locators=node-b:8888,node-a:9999 -spark.executor.cores=10 -snappydata.column.batchSize=100k -spark.sql.aqp.error=0.5
```
This sets the property for the snappy SQL shell's session.

| Properties | Description |
|--------|--------|
|snappydata.flushReservoirThreshold|Reservoirs of sample table will be flushed and stored in columnar format if sampling is done on the base table of size more than flushReservoirThreshold. The default value is10,000.</br> This property must be set in the *conf/servers* and *conf/leads* file.|
|spark.sql.aqp.numBootStrapTrials|Number of bootstrap trials to do for calculating error bounds. The default value is100. </br>This property must be set in the *conf/leads* file.|
|spark.sql.aqp.error|Maximum relative error tolerable in the approximate value calculation. It should be a fractional value not exceeding 1. The default value is0.2. </br>This property can be set as connection property in the Snappy SQL shell.|
|spark.sql.aqp.confidence|Confidence with which the error bounds are calculated for the approximate value. It should be a fractional value not exceeding 1. </br> The default value is0.95. </br>This property can be set as connection property in the Snappy SQL shell.|
|sparksql.aqp.behavior|The action to be taken if the error computed goes outside the error tolerance limit. The default value is`DO_NOTHING`. </br>This property can be set as connection property in the Snappy SQL shell.|

