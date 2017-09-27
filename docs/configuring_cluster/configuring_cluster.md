# Configuration

Configuration files for locator, lead, and server should be created in the **conf** folder located in the SnappyData home directory with names **locators**, **leads**, and **servers**.

To do so, you can copy the existing template files **servers.template**, **locators.template**, **leads.template**, and rename them to **servers**, **locators**, **leads**.
These files should contain the hostnames of the nodes (one per line) where you intend to start the member. You can modify the properties to configure individual members.

<a id="locator"></a>
## Configuring Locators

Locators provide discovery service for the cluster. Clients (e.g. JDBC) connect to the locator and discover the lead and data servers in the cluster. The clients automatically connect to the data servers upon discovery (upon initial connection). Cluster members (Data servers, Lead nodes) also discover each other using the locator. Refer to the [Architecture](../architecture.md) section for more information on the core components.

It is recommended to configure two locators (for HA) in production using the **conf/locators** file located in the **<_SnappyData_home_>/conf** directory. 

In this file, you can specify:

* The host name on which a SnappyData locator is started.

* The startup directory where the logs and configuration files for that locator instance are located.

* SnappyData specific properties that can be passed.

You can refer to the **conf/locators.template** file for some examples. 

### List of Locator Properties 

Refer to the [SnappyData properties](property_description.md) for the complete list of SnappyData properties.

|Property|Description|
|-|-|
|-J|JVM option passed to the spawned SnappyData server JVM. </br>For example, use -J-Xmx1024m to set the JVM heap to 1GB.|
|-dir|The working directory of the server that contains the SnappyData Server status file and the default location for the log file, persistent files, data dictionary, and so forth (defaults to the current directory).| 
|-classpath|Location of user classes required by the SnappyData Server.</br>This path is appended to the current classpath.|
|-heap-size|<a id="heap-size"></a> Sets the maximum heap size for the Java VM, using SnappyData default resource manager settings. </br>For example, -heap-size=1024m. </br>If you use the `-heap-size` option, by default SnappyData sets the critical-heap-percentage to 90% of the heap size, and the `eviction-heap-percentage` to 81% of the `critical-heap-percentage`. </br>SnappyData also sets resource management properties for eviction and garbage collection if they are supported by the JVM. |
|-locators|List of locators as comma-separated host:port values used to communicate with running locators in the system and thus discover other peers of the distributed system. </br>The list must include all locators in use and must be configured consistently for every member of the distributed system.|
|-bind-address|IP address on which the locator is bound. The default behavior is to bind to all local addresses.|
|-log-file|Path of the file to which this member writes log messages (default is snappyserver.log in the working directory)|
|-peer-discovery-address|Use this as value for port in the "host:port" value of "-locators" property |
|-peer-discovery-port|The port on which the locator listens for peer discovery (includes servers as well as other locators).  </br>Valid values are in the range 1-65535, with a default of 10334.|
|-member-timeout<a id="member-timeout"></a>|Uses the [member-timeout](../best_practices/setup_cluster.md#member-timeout) server configuration, specified in milliseconds, to detect the abnormal termination of members. The configuration setting is used in two ways:</br> 1) First, it is used during the UDP heartbeat detection process. When a member detects that a heartbeat datagram is missing from the member that it is monitoring after the time interval of 2 * the value of member-timeout, the detecting member attempts to form a TCP/IP stream-socket connection with the monitored member as described in the next case.</br> 2) The property is then used again during the TCP/IP stream-socket connection. If the suspected process does not respond to the are you alive datagram within the time period specified in member-timeout, the membership coordinator sends out a new membership view that notes the member's failure. </br>Valid values are in the range 1000..600000.|

<a id="locator-example"></a>
**Example**: To start two locators on node-a:9999 and node-b:8888, update the configuration file as follows:

```scala
$ cat conf/locators
node-a -peer-discovery-port=9999 -dir=/node-a/locator1 -heap-size=1024m -locators=node-b:8888
node-b -peer-discovery-port=8888 -dir=/node-b/locator2 -heap-size=1024m -locators=node-a:9999
```

<a id="lead"></a>
## Configuring Leads

Lead Nodes primarily runs the SnappyData managed Spark driver. There is one primary lead node at any given instance, but there can be multiple secondary lead node instances on standby for fault tolerance. Applications can run Jobs using the REST service provided by the Lead node. Most of the SQL queries are automatically routed to the Lead to be planned and executed through a scheduler. You can refer to the **conf/leads.template** file for some examples. 

Create the configuration file (**leads**) for leads in the **<_SnappyData_home_>/conf** directory.

### List of Lead Properties
Refer to the [SnappyData properties](property_description.md) for the complete list of SnappyData properties.

|Property|Description</br>|
|-|-|
|J|JVM option passed to the spawned SnappyData server JVM. </br>For example, use -J-Xmx1024m to set the JVM heap to 1GB.|
|dir|The working directory of the server that contains the SnappyData Server status file and the default location for the log file, persistent files, data dictionary, and so forth (defaults to the current directory).| 
|classpath|Location of user classes required by the SnappyData Server.</br>This path is appended to the current classpath.|
|heap-size|<a id="heap-size"></a> Sets the maximum heap size for the Java VM, using SnappyData default resource manager settings. </br>For example, -heap-size=1024m. </br>If you use the `-heap-size` option, by default SnappyData sets the critical-heap-percentage to 90% of the heap size, and the `eviction-heap-percentage` to 81% of the `critical-heap-percentage`. </br>SnappyData also sets resource management properties for eviction and garbage collection if they are supported by the JVM. |
|memory-size|<a id="memory-size"></a>Specifies the total memory that can be used by the node for column storage and execution in off-heap. Default value is 0 (OFF_HEAP is not used by default)|
|locators|List of locators as comma-separated host:port values used to communicate with running locators in the system and thus discover other peers of the distributed system. </br>The list must include all locators in use and must be configured consistently for every member of the distributed system.|
|bind-address|IP address on which the locator is bound. The default behaviour is to bind to all local addresses.|
|critical-heap-percentage|Sets the Resource Manager's critical heap threshold in percentage of the old generation heap, 0-100. </br>If you set `-heap-size`, the default value for `critical-heap-percentage` is set to 90% of the heap size. </br>Use this switch to override the default.</br>When this limit is breached, the system starts canceling memory-intensive queries, throws low memory exceptions for new SQL statements, and so forth, to avoid running out of memory.|
|eviction-heap-percentage|Sets the memory usage percentage threshold (0-100) that the Resource Manager will use to start evicting data from the heap. By default, the eviction threshold is 81% of whatever is set for `-critical-heap-percentage`.</br>Use this switch to override the default.</br>|
|log-file|Path of the file to which this member writes log messages (default is snappyserver.log in the working directory)|
|member-timeout<a id="member-timeout"></a>|Uses the [member-timeout](../best_practices/setup_cluster.md#member-timeout) server configuration, specified in milliseconds, to detect the abnormal termination of members. The configuration setting is used in two ways:</br> 1) First, it is used during the UDP heartbeat detection process. When a member detects that a heartbeat datagram is missing from the member that it is monitoring after the time interval of 2 * the value of member-timeout, the detecting member attempts to form a TCP/IP stream-socket connection with the monitored member as described in the next case.</br> 2) The property is then used again during the TCP/IP stream-socket connection. If the suspected process does not respond to the are you alive datagram within the time period specified in member-timeout, the membership coordinator sends out a new membership view that notes the member's failure. </br>Valid values are in the range 1000..600000.|
|snappydata.column.batchSize|The default size of blocks to use for storage in the SnappyData column store. The default value is 24M.|
|spark.driver.maxResultSize|Limit of the total size of serialized results of all partitions for each action (e.g. collect). The value should be at least 1M, or 0 for unlimited. Jobs will be aborted if the total size of results is above this limit. Having a high limit may cause out-of-memory errors in the lead.|
|spark.executor.cores|The number of cores to use on each server. |
|spark.network.timeout|The default timeout for all network interactions while running queries. |
|spark.local.dir|Directory to use for "scratch" space in SnappyData, including map output files and RDDs that get stored on disk. This should be on a fast, local disk in your system. It can also be a comma-separated list of multiple directories on different disks.|
|spark.ui.port|Port for your SnappyData Pulse, which shows tables, memory and workload data. Default is 5050|

**Example**: To start a lead (node-l), set `spark.executor.cores` as 10 on all servers, and change the Spark UI port from 5050 to 9090, update the configuration file as follows:

```
$ cat conf/leads
# This goes to the default directory 
node-l -heap-size=4096m -spark.ui.port=9090 -locators=node-b:8888,node-a:9999 -spark.executor.cores=10
```

<a id="dataserver"></a>
## Configuring Data Servers
Data Servers hosts data, embeds a Spark executor, and also contains a SQL engine capable of executing certain queries independently and more efficiently than Spark engine. Data servers use intelligent query routing to either execute the query directly on the node or to pass it to the lead node for execution by Spark SQL. You can refer to the **conf/servers.template** file for some examples. 

Create the configuration file (**servers**) for data servers in the **<_SnappyData_home_>/conf** directory. 

### List of Server Properties
Refer to the [SnappyData properties](property_description.md) for the complete list of SnappyData properties.

|Property|Description</br>|
|-|-|-|
|-J|JVM option passed to the spawned SnappyData server JVM. </br>For example, use -J-Xmx1024m to set the JVM heap to 1GB.|
|-dir|The working directory of the server that contains the SnappyData Server status file and the default location for the log file, persistent files, data dictionary, and so forth (defaults to the current directory).|
|-classpath|Location of user classes required by the SnappyData Server.</br>This path is appended to the current classpath.|
|-heap-size|<a id="heap-size"></a> Sets the maximum heap size for the Java VM, using SnappyData default resource manager settings. </br>For example, -heap-size=1024m. </br>If you use the `-heap-size` option, by default SnappyData sets the critical-heap-percentage to 90% of the heap size, and the `eviction-heap-percentage` to 81% of the `critical-heap-percentage`. </br>SnappyData also sets resource management properties for eviction and garbage collection if they are supported by the JVM. |
|-memory-size|<a id="memory-size"></a>Specifies the total memory that can be used by the node for column storage and execution in off-heap. Default value is 0 (OFF_HEAP is not used by default)|
|-locators|List of locators as comma-separated host:port values used to communicate with running locators in the system and thus discover other peers of the distributed system. </br>The list must include all locators in use and must be configured consistently for every member of the distributed system.|
|<a id="rebalance"></a>-rebalance|Causes the new member to trigger a rebalancing operation for all partitioned tables in the system. </br>The system always tries to satisfy the redundancy of all partitioned tables on new member startup regardless of this option.|
|-bind-address|IP address on which the locator is bound. The default behavior is to bind to all local addresses.|
|-critical-heap-percentage|Sets the Resource Manager's critical heap threshold in percentage of the old generation heap, 0-100. </br>If you set `-heap-size`, the default value for `critical-heap-percentage` is set to 90% of the heap size. </br>Use this switch to override the default.</br>When this limit is breached, the system starts canceling memory-intensive queries, throws low memory exceptions for new SQL statements, and so forth, to avoid running out of memory.|
|-eviction-heap-percentage|Sets the memory usage percentage threshold (0-100) that the Resource Manager will use to start evicting data from the heap. By default, the eviction threshold is 81% of whatever is set for `-critical-heap-percentage`.</br>Use this switch to override the default.</br>|
|-critical-off-heap-percentage|Sets the critical threshold for off-heap memory usage in percentage, 0-100. </br>When this limit is breached, the system starts canceling memory-intensive queries, throws low memory exceptions for new SQL statements, and so forth, to avoid running out of off-heap memory.|
|-eviction-off-heap-percentage|Sets the off-heap memory usage percentage threshold, 0-100, that the Resource Manager uses to start evicting data from off-heap memory. </br>By default, the eviction threshold is 81% of whatever is set for `-critical-off-heap-percentage`. </br>Use this switch to override the default.|
|-log-file|Path of the file to which this member writes log messages (default is snappyserver.log in the working directory)|
|-J-Dgemfirexd.hostname-for-clients|Hostname or IP address that is sent to clients so they can connect to the locator. The default is the `bind-address` of the locator.|
|-member-timeout<a id="member-timeout"></a>|Uses the [member-timeout](../best_practices/setup_cluster.md#member-timeout) server configuration, specified in milliseconds, to detect the abnormal termination of members. The configuration setting is used in two ways:</br> 1) First, it is used during the UDP heartbeat detection process. When a member detects that a heartbeat datagram is missing from the member that it is monitoring after the time interval of 2 * the value of member-timeout, the detecting member attempts to form a TCP/IP stream-socket connection with the monitored member as described in the next case.</br> 2) The property is then used again during the TCP/IP stream-socket connection. If the suspected process does not respond to the are you alive datagram within the time period specified in member-timeout, the membership coordinator sends out a new membership view that notes the member's failure. </br>Valid values are in the range 1000..600000.|
|<a id="thrift-properties"></a>thrift-ssl|Specifies if you want to enable or disable SSL. Values: true or false|
|thrift-ssl-properties|Comma-separated SSL properties including:</br>`protocol`: default "TLS",</br>`enabled-protocols`: enabled protocols separated by ":"</br>`cipher-suites`: enabled cipher suites separated by ":"</br>`client-auth`=(true or false): if client also needs to be authenticated </br>`keystore`: path to key store file </br>`keystore-type`: the type of key-store (default "JKS") </br>`keystore-password`: password for the key store file</br>`keymanager-type`: the type of key manager factory </br>`truststore`: path to trust store file</br>`truststore-type`: the type of trust-store (default "JKS")</br>`truststore-password`: password for the trust store file </br>`trustmanager-type`: the type of trust manager factory </br> |


**Example**: To start a two servers (node-c and node-c), update the configuration file as follows:

```
$ cat conf/servers
node-c -dir=/node-c/server1 -heap-size=4096m -memory-size=16g -locators=node-b:8888,node-a:9999
node-c -dir=/node-c/server2 -heap-size=4096m -memory-size=16g -locators=node-b:8888,node-a:9999
```
## Configuring SnappyData Smart Connector  

Spark applications run as independent sets of processes on a cluster, coordinated by the SparkContext object in your main program (called the driver program). In Smart connector mode, a Spark application connects to SnappyData cluster to store and process data. SnappyData currently works with Spark version 2.1.1. To work with SnappyData cluster, a Spark application has to set the snappydata.connection property while starting.   

| Property |Description |
|--------|--------|
| snappydata.connection        |SnappyData cluster's locator host and JDBC client port on which locator listens for connections. Has to be specified while starting a Spark application.|

**Example**:

```bash
$ ./bin/spark-submit --deploy-mode cluster --class somePackage.someClass  
	--master spark://localhost:7077 --conf spark.snappydata.connection=localhost:1527 
	--packages "SnappyDataInc:snappydata:1.0.0-s_2.11" 
```

<a id="logging"></a>
## Logging 

Currently, log files for SnappyData components go inside the working directory. To change the log file directory, you can specify a property _-log-file_ as the path of the directory. </br>
The logging levels can be modified by adding a *conf/log4j.properties* file in the product directory. 

```bash
$ cat conf/log4j.properties 
log4j.logger.org.apache.spark.scheduler.DAGScheduler=DEBUG
log4j.logger.org.apache.spark.scheduler.TaskSetManager=DEBUG
```
!!! Note:
	For a set of applicable class names and default values see the file **conf/log4j.properties.template**, which can be used as a starting point. Consult the [log4j 1.2.x documentation](http://logging.apache.org/log4j/) for more details on the configuration file.

## SnappyData Command Line Utility

Instead of starting SnappyData cluster using SSH scripts, individual components can be configured, started and stopped on a system locally using these commands.

```
$ bin/snappy locator start  -dir=/node-a/locator1 
$ bin/snappy server start  -dir=/node-b/server1  -locators=localhost[10334] -heap-size=16g 
$ bin/snappy leader start  -dir=/node-c/lead1  -locators=localhost[10334] -spark.executor.cores=32

$ bin/snappy locator stop -dir=/node-a/locator1
$ bin/snappy server stop -dir=/node-b/server1
$ bin/snappy leader stop -dir=/node-c/lead1
```
