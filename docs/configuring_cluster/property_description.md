# List of Properties

The following list of commonly used configuration properties can be set to configure the cluster.  These properties can be set in the **conf/servers**, **conf/leads** or **conf/locators** configuration files.

*	[Network Configuration](#network)
*	[Memory Configuration](#memory)
*	[Disk Configuration](#disk)
*	[Security Configuration](#security)
*	[Spark Configuration](#spark)
*	[Logging, Metrics Configuration](#logging)
*	[JVM Properties](#jvm)
*	[SQL Properties](#sql-properties)
*	[AQP Properties](#aqp)
*	[Connection Properties](#connectionpro)

!!! Tip
	For system properties (set in the conf/lead, conf/servers and conf/locators file), -D and -XX: can be used. -J is NOT required for -D and -XX options.

<a id="network"></a>
## Network Configuration

|Property|Description|Components</br>|
|-|-|-|
|-ack-severe-alert-threshold| See [ack-severe-alert-threshold](../reference/configuration_parameters/ack-severe-alert-threshold.md)|
|-ack-wait-threshold| See [ack-wait-threshold](../reference/configuration_parameters/ack-wait-threshold.md)| |
|-bind-address|IP address on which the member is bound. The default behavior is to bind to all local addresses. Also see [bind-address](../reference/configuration_parameters/bind-address.md) |Server</br>Lead</br>Locator|
|-client-port| The port that the network controller listens for client connections in the range of 1 to 65535. The default value is 1527.|Locator</br>Server|
|-hostname-for-clients<a id="host-name"></a>|Set the IP address or host name that this server/locator sends to JDBC/ODBC/thrift clients to use for connection. The default value causes the client-bind-address to be given to clients. This value can be different from client-bind-address for cases where locators, servers are behind a NAT firewall (AWS for example) where client-bind-address needs to be a private one that gets exposed to clients outside the firewall as a different public address specified by this property. In many cases this is handled by hostname translation itself, i.e. hostname used in client-bind-address resolves to internal IP address from inside but to public IP address from outside, but for other cases this property will be required.|Server|
|-enable-network-partition-detection|See [enable-network-partition-detection](../reference/configuration_parameters/enable-network-partition-detection.md)||
|-enforce-unique-host|See [enforce-unique-host](../reference/configuration_parameters/enforce-unique-host.md)||
|-locators|List of locators as comma-separated host:port values used to communicate with running locators in the system and thus discover other peers of the distributed system. </br>The list must include all locators in use and must be configured consistently for every member of the distributed system. This property should be configured for all the nodes in the respective configuration files, if there are multiple locators.|Server</br>Lead</br>Locator|
|-member-timeout<a id="member-timeout"></a>|Uses the member-timeout server configuration, specified in milliseconds, to detect the abnormal termination of members. The configuration setting is used in two ways:</br> 1) First, it is used during the UDP heartbeat detection process. When a member detects that a heartbeat datagram is missing from the member that it is monitoring after the time interval of 2 * the value of member-timeout, the detecting member attempts to form a TCP/IP stream-socket connection with the monitored member as described in the next case.</br> 2) The property is then used again during the TCP/IP stream-socket connection. If the suspected process does not respond to the **are you alive** datagram within the time period specified in member-timeout, the membership coordinator sends out a new membership view that notes the member's failure. </br>Valid values are in the range 1000-600000 milliseconds. For more information, refer to [Best Practices](../best_practices/important_settings.md#member-timeout)|Server</br>Lead</br>Locator|
|-membership-port-range|See [membership-port-range](../reference/configuration_parameters/membership-port-range.md)
|-peer-discovery-address|Use this as value for the port in the "host:port" value of "-locators" property |Locator|
|-peer-discovery-port|Port on which the locator listens for peer discovery (includes servers as well as other locators).  </br>Valid values are in the range 1-65535, with a default of 10334.|Locator|
|read-timeout| See [read-timeout](../reference/configuration_parameters/read-timeout.md)| |
|-spark.ui.port|Port for your SnappyData Monitoring Console, which shows tables, memory and workload data. The default is 5050|Lead|
|-redundancy-zone|See [redundancy-zone](../reference/configuration_parameters/redundancy-zone.md) | |
|-secondary-locators|See [secondary-locators](../reference/configuration_parameters/secondary-locators.md) | |
|-socket-buffer-size| See [socket-buffer-size](../reference/configuration_parameters/socket-buffer-size.md) | |
|-socket-lease-time|See [socket-lease-time](../reference/configuration_parameters/socket-lease-time.md) | |
|-gemfirexd.max-lock-wait|See [gemfirexd.max-lock-wait](../reference/configuration_parameters/snappydata.max-lock-wait.md) |

<a id="memory"></a>
## Memory Configuration

|Property|Description|Components</br>|
|-|-|-|
|-critical-heap-percentage<a id="critical-heap-percentage"></a>|Sets the Resource Manager's critical heap threshold in percentage of the old generation heap, 0-100. </br>If you set `-heap-size`, the default value for `critical-heap-percentage` is set to 95% of the heap size. </br>Use this switch to override the default.</br>When this limit is breached, the system starts canceling memory-intensive queries, throws low memory exceptions for new SQL statements, and so forth, to avoid running out of memory.|Server</br>Lead|
|-critical-off-heap-percentage|Sets the critical threshold for off-heap memory usage in percentage, 0-100. </br>When this limit is breached, the system starts canceling memory-intensive queries, throws low memory exceptions for new SQL statements, and so forth, to avoid running out of off-heap memory.|Server|
|-eviction-heap-percentage|Sets the memory usage percentage threshold (0-100) that the Resource Manager will use to start evicting data from the heap. By default, the eviction threshold is 85.5% of whatever is set for `-critical-heap-percentage`.</br>Use this switch to override the default.|Server</br>Lead</br>|
|-eviction-off-heap-percentage|Sets the off-heap memory usage percentage threshold, 0-100, that the Resource Manager uses to start evicting data from off-heap memory. </br>By default, the eviction threshold is 85.5% of whatever is set for `-critical-off-heap-percentage`. </br>Use this switch to override the default.|Server|
|-heap-size|<a id="heap-size"></a> Sets the maximum heap size for the Java VM, using SnappyData default resource manager settings. </br>For example, -heap-size=1GB. </br>If you use the `-heap-size` option, by default SnappyData sets the critical-heap-percentage to 95% of the heap size, and the `eviction-heap-percentage` to 85.5% of the `critical-heap-percentage`. </br>SnappyData also sets resource management properties for eviction and garbage collection if they are supported by the JVM. |Server</br>Lead</br>Locator|
|-memory-size|<a id="memory-size"></a>Specifies the total memory that can be used by the node for column storage and execution in off-heap. The default value is either 0 or it gets auto-configured in [specific scenarios](../configuring_cluster/configuring_cluster.md#autoconfigur_offheap). |Server</br>Lead|
|<a id="sparkdrivermaxresult"></a>-spark.driver.maxResultSize|Limit of the total size of serialized results of all partitions for each action (e.g. collect). The value should be at least 1MB or 0 for unlimited. Jobs will be aborted if the total size of results is above this limit. Having a high limit may cause out-of-memory errors in the lead. The default max size is 1GB. |Lead|

<a id="disk"></a>
## Disk Configuration

|Property|Description|Components</br>|
|-|-|-|
|-archive-disk-space-limit|See [archive-disk-space-limit](../reference/configuration_parameters/archive-disk-space-limit.md)|
|-archive-file-size-limit|See [archive-file-size-limit](../reference/configuration_parameters/archive-file-size-limit.md)|
|-dir|Working directory of the member that contains the SnappyData Server status file and the default location for the log file, persistent files, data dictionary, and so forth (defaults to the current directory).| Server</br>Lead</br>Locator</br>|
|gemfirexd.default-startup-recovery-delay|See [gemfirexd.default-startup-recovery-delay](../reference/configuration_parameters/snappydata.default-startup-recovery-delay.md)|
|-spark.local.dir|Directory to use for "scratch" space in SnappyData, including map output files and RDDs that get stored on disk. This should be on a fast, local disk in your system. It can also be a comma-separated list of multiple directories on different disks. For more information, refer to [Best Practices](../best_practices/important_settings.md#spark-local-dir).|Lead</br>Server|
|-sys-disk-dir|See [sys-disk-dir](../reference/configuration_parameters/sys-disk-dir.md)|


<a id="security"></a>
## Security Configuration

|Property|Description|Components</br>|
|-|-|-|
|-DCHECK_EXTERNAL_TABLE_AUTHZ | Enable authorization of external tables by setting this system property to true when the cluster's security is enabled. System admin or the schema owner can grant or revoke the permissions on external tables to other users.|Lead|
|-Dsnappydata.enable-rls|Enables the system for row level security when set to true.  By default this is off. If this property is set to true,  then the Smart Connector access to SnappyData fails.|Server</br>Lead</br>Locator
|-Dsnappydata.RESTRICT_TABLE_CREATION|Applicable when security is enabled in the cluster. If true, users cannot execute queries (including DDLs and DMLs) even in their default or own schema unless cluster admin explicitly grants them the required permissions using GRANT command. The default is false. |Server</br>Lead</br>Locator|
|-spark.ssl.enabled<a id="ssl_spark_enabled"></a>|Enables or disables Spark layer encryption. The default is false. |Lead|
|-spark.ssl.keyPassword<a id="ssl_spark_password"></a>|The password to the private key in the key store. |Lead|
|-spark.ssl.keyStore<a id="ssl_spark_keystore"></a>|Path to the key store file. The path can be absolute or relative to the directory in which the process is started.|Lead|
|-spark.ssl.keyStorePassword<a id="ssl_spark_keystorpass"></a>|The password used to access the keystore. |Lead|
|-spark.ssl.trustStore<a id="ssl_spark_trustore"></a>|Path to the trust store file. The path can be absolute or relative to the directory in which the process is started. |Lead|
|-spark.ssl.trustStorePassword<a id="truststorepassword"></a>|The password used to access the truststore. |Lead|
|-spark.ssl.protocol<a id="ssl_spark_ssl_protocol"></a>|The protocol that must be supported by JVM. For example, TLS.|Lead|
|SSL Configuration for P2P|[ssl-enabled](../reference/configuration_parameters/ssl_enabled.md), [ssl-ciphers](../reference/configuration_parameters/ssl_ciphers.md), [ssl-protocols](../reference/configuration_parameters/ssl_protocols.md), [ssl-require-authentication](../reference/configuration_parameters/ssl_require_auth.md). </br> These properties need not be added to  the Lead members in case of a client-server connection.|Server</br>Lead</br>Locator</br>|
|-thrift-ssl<a id="thrift-properties"></a>|Specifies if you want to enable or disable SSL. Values are true or false.|Server</br>Lead</br>Locator</br>
|-thrift-ssl-properties|Comma-separated SSL properties including:</br>`protocol`: default "TLS",</br>`enabled-protocols`: enabled protocols separated by ":"</br>`cipher-suites`: enabled cipher suites separated by ":"</br>`client-auth`=(true or false): if client also needs to be authenticated </br>`keystore`: Path to key store file </br>`keystore-type`: The type of key-store (default "JKS") </br>`keystore-password`: Password for the key store file</br>`keymanager-type`: The type of key manager factory </br>`truststore`: Path to trust store file</br>`truststore-type`: The type of trust-store (default "JKS")</br>`truststore-password`: Password for the trust store file </br>`trustmanager-type`: The type of trust manager factory </br> |Server|

<a id="spark"></a>
## Spark Configuration

|Property|Description|Components</br>|
|-|-|-|
|-spark.executor.cores|The number of cores to use on each server. |Lead|
|-spark.context-settings.num-cpu-cores| The number of cores that can be allocated. The default is 4. |Lead|
|-spark.context-settings.memory-per-node| The executor memory per node (-Xmx style. For example: 512m, 1G). The default is 512m. |Lead|
|-spark.context-settings.streaming.batch_interval| The batch interval for Spark Streaming contexts in milliseconds. The default is 1000.|Lead|
|-spark.context-settings.streaming.stopGracefully| If set to true, the streaming stops gracefully by waiting for the completion of processing of all the received data. The default is true.|Lead|
|-spark.context-settings.streaming.stopSparkContext| if set to true, the SparkContext is stopped along with the StreamingContext. The default is true. |Lead|
|<a id="sparkdrivermaxresult"></a>-spark.driver.maxResultSize|Limit of the total size of serialized results of all partitions for each action (e.g. collect). The value should be at least 1MB or 0 for unlimited. Jobs will be aborted if the total size of results is above this limit. Having a high limit may cause out-of-memory errors in the lead. The default max size is 1GB. |Lead|
|-spark.eventLog.enabled| Set to True to enable event logging for Spark jobs.| Lead|
|-spark.eventLog.dir| Specify the directory path where the events can be logged for Spark jobs.| Lead|
|-spark.jobserver.bind-address|The address on which the jobserver listens. Default address is 0.0.0.|Lead|
|-spark.jobserver.job-result-cache-size|The number of job results to keep per JobResultActor/context. The default is 5000.|Lead|
|-spark.jobserver.max-jobs-per-context|The number of jobs that can be run simultaneously in the context. The default is 8.|Lead|
|-spark.jobserver.port|The port on which to run the jobserver. Default port is 8090.|Lead|
|-spark.local.dir|Directory to use for "scratch" space in SnappyData, including map output files and RDDs that get stored on disk. This should be on a fast, local disk in your system. It can also be a comma-separated list of multiple directories on different disks. For more information, refer to [Best Practices](../best_practices/important_settings.md#spark-local-dir).|Lead</br>Server|
|-spark.network.timeout|The default timeout for all network interactions while running queries.|Lead|
|-spark.sql.autoBroadcastJoinThreshold|Configures the maximum size in bytes for a table that is broadcast to all server nodes when performing a join.  By setting this value to **-1** broadcasting can be disabled. |Lead|
|-spark.sql.codegen.cacheSize<a id="codegencache"></a>|Size of the generated code cache. This effectively controls the maximum number of query plans whose generated code (Classes) is cached. The default is 2000. |Lead|
|-spark.sql.codegen.wholeStage<a id="wholeStageCodeGen"></a>|Turn ON/OFF whole stage code generation in Spark/Snappy SQL. The default is True. |Lead|
|-spark.sql.aqp.numBootStrapTrials|Number of bootstrap trials to do for calculating error bounds. The default value is100. </br>This property must be set in the **conf/leads** file.|Lead|
|-spark.sql.aqp.error|Maximum relative error tolerable in the approximate value calculation. It should be a fractional value not exceeding 1. The default value is0.2. </br>This property can be set as connection property in the Snappy SQL shell.|Lead|
|-spark.sql.aqp.confidence|Confidence with which the error bounds are calculated for the approximate value. It should be a fractional value not exceeding 1. </br> The default value is0.95. </br>This property can be set as connection property in the Snappy SQL shell.|Lead|
|-spark.sql.aqp.behavior|The action to be taken if the error computed goes outside the error tolerance limit. The default value is`DO_NOTHING`. </br>This property can be set as connection property in the Snappy SQL shell.|Lead|
|-spark.ssl.enabled<a id="ssl_spark_enabled"></a>|Enables or disables Spark layer encryption. The default is false. |Lead|
|-spark.ssl.keyPassword<a id="ssl_spark_password"></a>|The password to the private key in the key store. |Lead|
|-spark.ssl.keyStore<a id="ssl_spark_keystore"></a>|Path to the key store file. The path can be absolute or relative to the directory in which the process is started.|Lead|
|-spark.ssl.keyStorePassword<a id="ssl_spark_keystorpass"></a>|The password used to access the keystore. |Lead|
|-spark.ssl.trustStore<a id="ssl_spark_trustore"></a>|Path to the trust store file. The path can be absolute or relative to the directory in which the process is started. |Lead|
|-spark.ssl.trustStorePassword<a id="truststorepassword"></a>|The password used to access the truststore. |Lead|
|-spark.ssl.protocol<a id="ssl_spark_ssl_protocol"></a>|The protocol that must be supported by JVM. For example, TLS.|Lead|
|-spark.ui.port|Port for your SnappyData Monitoring Console, which shows tables, memory and workload data. The default is 5050|Lead|

<a id="logging"></a>
## Logging, Metrics Configuration

|Property|Description|Components</br>|
|-|-|-|
|-enable-stats| See [enable-stats](../reference/configuration_parameters/enable-stats.md) | |
|-enable-time-statistics| See [enable-time-statisticss](../reference/configuration_parameters/enable-time-statistics.md) | |
|-enable-timestats| See [enable-timestats](../reference/configuration_parameters/enable-timestats.md) | |
|-gemfirexd.debug.true| Use this property to set the required [trace flag](../monitoring/configure_logging.md#trace-flag) which enables the logging of specific features of SnappyData.|Server</br>Lead</br>Locator|
|-log-file|Path of the file to which this member writes log messages (default is snappy[member].log in the working directory. For example, **snappylocator.log**, **snappyleader.log**,**snappyserver.log**. In case logging is set via log4j, the default log file is **snappydata.log** for each of the SnappyData member.)|Server</br>Lead</br>Locator|
|-log-level|See [log-level](../reference/configuration_parameters/log-level.md) | |
|-snappy.history| See [snappy.history](../reference/configuration_parameters/snappy.history.md) | |
|-statistic-archive-file| See [statistic-archive-file](../reference/configuration_parameters/statistic-archive-file.md) | |
|-statistic-sample-rate| See [statistic-sample-rate](../reference/configuration_parameters/statistic-archive-file.md) | |
|-statistic-sampling-enabled| See [statistic-sampling-enabled](../reference/configuration_parameters/statistic-sampling-enabled.md) | |

<a id="jvm"></a>
## JVM Properties

|Property|Description|Components</br>|
|-|-|-|
|-classpath|Location of user classes required by the SnappyData Server.</br>This path is appended to the current classpath.|Server</br>Lead</br>Locator|
|-J|JVM option passed to the spawned SnappyData server JVM. </br>For example, use -J-Xmx1GB to set the JVM heap to 1GB.|Server</br>Lead</br>Locator|


Other than the above properties, you can also refer the [Configuration Parameters section](../reference/configuration_parameters/index.md#property-names) for properties that are used in special cases.

<a id="sql-properties"></a>
## SQL Properties

These properties can be set using a `SET` SQL command or using the configuration properties in the **conf/leads** file. The `SET` SQL command sets the property for the current SnappySession while setting it in **conf/leads** file sets the property for all SnappySession.

For example: Set in the snappy SQL shell

```pre
snappy> connect client 'localhost:1527';
snappy> set snappydata.column.batchSize=100k;
```
This sets the property for the snappy SQL shell's session.

Set in the **conf/leads** file
```pre
$ cat conf/leads
node-l -heap-size=4096m -spark.ui.port=9090 -locators=node-b:8888,node-a:9999 -spark.executor.cores=10 -snappydata.column.batchSize=100k
```

| Property | Description|
|--------|--------|
|-allow-explicit-commit|See [allow-explicit-commit](../reference/configuration_parameters/allow-explicit-commit.md) | |
|-init-scripts|See [init-scripts](../reference/configuration_parameters/init-scripts.md)| |
|-skip-constraint-checks|See [skip-constraint-checks](../reference/configuration_parameters/skip-constraint-checks.md)| |
|-skip-locks|See [skip-locks](../reference/configuration_parameters/skip-locks.md)| |
|-gemfirexd.datadictionary.allow-startup-errors|See [gemfirexd.datadictionary.allow-startup-errors](../reference/configuration_parameters/snappydata.datadictionary.allow-startup-errors.md)| |
|-gemfirexd.query-cancellation-interval|See [gemfirexd.query-cancellation-interval](../reference/configuration_parameters/snappydata.query-cancellation-interval.md)| |
|-gemfirexd.query-timeout| See [gemfirexd.query-timeout](../reference/configuration_parameters/snappydata.query-timeout.md)||
|-snappydata.column.batchSize |The default size of blocks to use for storage in SnappyData column and store. When inserting data into the column storage this is the unit (in bytes or k/m/g suffixes for unit) that is used to split the data into chunks for efficient storage and retrieval. </br> This property can also be set for each table in the `create table` DDL. Maximum allowed size is 2GB. The default is 24m.|
|-snappydata.column.maxDeltaRows|The maximum number of rows that can be in the delta buffer of a column table. The size of the delta buffer is already limited by `ColumnBatchSize` property, but this allows a lower limit on the number of rows for better scan performance. So the delta buffer is rolled into the column store whichever of `ColumnBatchSize` and this property is hit first. It can also be set for each table in the `create table` DDL, else this setting is used for the `create table`|
|-snappydata.hiveServer.enabled|Enables the Hive Thrift server for SnappyData.This is enabled by default when you start the cluster. Thus it adds an additional 10 seconds to the cluster startup time. To avoid this additional time, you can set the property to false.|
|snappydata.maxRetryAttemptsForWrite|Default retry of Spark tasks on failure can cause duplicates in the case of insert operations. This property can be set to **0** to avoid this scenario. Other operations, as usual, retries without causing any consistency issues.|
|-snappydata.sql.hashJoinSize|The join would be converted into a hash join if the table is of size less than the `hashJoinSize`.  The limit specifies an estimate on the input data size (in bytes or k/m/g/t suffixes for unit). The default value is 100MB.|
|-snappydata.sql.hashAggregateSize|Aggregation uses optimized hash aggregation plan but one that does not overflow to disk and can cause OOME if the result of aggregation is large. The limit specifies the input data size (in bytes or k/m/g/t suffixes for unit) and not the output size. Set this only if there are queries that can return a large number of rows in aggregation results. The default value is set to 0 which means, no limit is set on the size, so the optimized hash aggregation is always used.|
|-snappydata.sql.planCacheSize|Number of query plans that will be cached.|
|-spark.sql.autoBroadcastJoinThreshold|Configures the maximum size in bytes for a table that is broadcast to all server nodes when performing a join.  By setting this value to **-1** broadcasting can be disabled. |
|-snappydata.linkPartitionsToBuckets|When this property is set to true, each bucket is always treated as a separate partition in column/row table scans. When this is set to false, SnappyData creates only as many partitions as executor cores by clubbing multiple buckets into each partition when possible. The default is false.|
|-snappydata.preferPrimaries|Use this property to configure your preference to use primary buckets in queries. This reduces the scalability of queries in the interest of reduced memory usage for secondary buckets. The default is false.|
|-snappydata.sql.partitionPruning|Use this property to set/unset the partition pruning of queries.|
|-snappydata.sql.tokenize|Use this property to enable/disable tokenization.|
|-snappydata.cache.putIntoInnerJoinResultSize| Use this property with extreme limits such as 1K and 10GB. The default is 100 MB.|
|-snappydata.scheduler.pool|Use this property to define scheduler pool to either default or low latency. You can also assign queries to different pools.|
|-snappydata.enable-experimental-features|Use this property to enable and disable experimental features. You can call out in case some features are completely broken and need to be removed from the product.|
|-snappydata.sql.planCaching|Use this property to enable/disable plan caching. By default it is disabled. |Lead|
|snappydata.recovery.enableTableCountInUI|In the recovery mode, by default, the table counts and sizes do not appear on the UI. To view the table counts, you should set this property to **true** in the lead's conf file. By default, the property is set to **false**, and the table count is shown as **-1**. ||
|sync-commits| See [sync-commits](../reference/configuration_parameters/sync-commits.md)||

<a id="aqp"></a>
## AQP Properties

The [AQP](../sde/index.md) properties can be set using a Snappy SQL shell (snappy-sql) command or using the configuration properties in the **conf/leads** file. </br>
The command sets the property for the current SnappySession while setting it in **conf/leads** file sets the property for all SnappySession.

For example: Set in the  Snappy SQL shell (snappy-sql)

```pre
snappy> connect client 'localhost:1527';
snappy> set snappydata.flushReservoirThreshold=20000;
```
Set in the **conf/leads** file
```pre
$ cat conf/leads
node-l -heap-size=4096m -spark.ui.port=9090 -locators=node-b:8888,node-a:9999 -spark.executor.cores=10 -snappydata.column.batchSize=100k -spark.sql.aqp.error=0.5
```
This sets the property for the snappy SQL shell's session.

| Properties | Description |
|--------|--------|
|-snappydata.flushReservoirThreshold|Reservoirs of sample table will be flushed and stored in columnar format if sampling is done on the base table of size more than flushReservoirThreshold. The default value is10,000.</br> This property must be set in the **conf/servers** and **conf/leads** file.|
|-spark.sql.aqp.numBootStrapTrials|Number of bootstrap trials to do for calculating error bounds. The default value is100. </br>This property must be set in the **conf/leads** file.|
|-spark.sql.aqp.error|Maximum relative error tolerable in the approximate value calculation. It should be a fractional value not exceeding 1. The default value is0.2. </br>This property can be set as connection property in the Snappy SQL shell.|
|-spark.sql.aqp.confidence|Confidence with which the error bounds are calculated for the approximate value. It should be a fractional value not exceeding 1. </br> The default value is0.95. </br>This property can be set as connection property in the Snappy SQL shell.|
|-spark.sql.aqp.behavior|The action to be taken if the error computed goes outside the error tolerance limit. The default value is`DO_NOTHING`. </br>This property can be set as connection property in the Snappy SQL shell.|

<a id="connectionpro"></a>
## Connection Properties

You can define connection properties directly in the JDBC connection URL, or in the Properties object while using JDBC API `DriverManager.getConnection(String url, java.util.Properties info)`. You can also define connection properties  in the `connect` command in an interactive SnappyData session using snappy shell.
An example URL that defines a connection property is shown below. In the URL, replace the `property1=value1` string with appropriate property that you want to use. Multiple properties can be be specified by separating them with a semicolon.

Example URL:

	jdbc:snappydata://locatorHostName:1527/property1=value1;property2=value2

Example connect command that sets connection properties while using snappy shell

	snappy> connect client 'localhost:1527/property1=value1;property2=value2';

|Property|Description|Components</br>|
|-|-|-|
|-allow-explicit-commit| See [allow-explicit-commit](../reference/configuration_parameters/allow-explicit-commit.md)||
|-enable-stats|See [enable-stats](../reference/configuration_parameters/enable-stats.md)||
|-enable-timestats|See [enable-timestats](../reference/configuration_parameters/enable-timestats.md)||
|-load-balance|See [load-balance](../reference/configuration_parameters/load-balance.md)||
|-log-file|See [log-file](../reference/configuration_parameters/log-file.md)||
|-password|See [password](../reference/configuration_parameters/password.md)||
|-read-timeout|See [read-timeout](../reference/configuration_parameters/read-timeout.md)||
|-skip-constraint-checks|See [skip-constraint-checks](../reference/configuration_parameters/skip-constraint-checks.md)||
|-skip-locks|See [skip-locks](../reference/configuration_parameters/skip-locks.md)||
|-user|See [user](../reference/configuration_parameters/user.md)||
