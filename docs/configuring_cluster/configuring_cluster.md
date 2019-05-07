# Configuration

Configuration files for locator, lead, and server should be created in the **conf** folder located in the SnappyData home directory with names **locators**, **leads**, and **servers**.

To do so, you can copy the existing template files **servers.template**, **locators.template**, **leads.template**, and rename them to **servers**, **locators**, **leads**.
These files should contain the hostnames of the nodes (one per line) where you intend to start the member. You can modify the properties to configure individual members.

!!! Tip
	- For system properties (set in the conf/lead, conf/servers and conf/locators file), -D and -XX: can be used. All other JVM properties need the `-J` prefix.

    - Instead of starting the SnappyData cluster, you can [start](../howto/start_snappy_cluster.md) and [stop](../howto/stop_snappy_cluster.md) individual components on a system locally.

<a id="locator"></a>
## Configuring Locators

Locators provide discovery service for the cluster. Clients (for example, JDBC) connect to the locator and discover the lead and data servers in the cluster. The clients automatically connect to the data servers upon discovery (upon initial connection). Cluster members (Data servers, Lead nodes) also discover each other using the locator. Refer to the [Architecture](../architecture.md) section for more information on the core components.

It is recommended to configure two locators (for HA) in production using the **conf/locators** file located in the **<_SnappyData_home_>/conf** directory. 

In this file, you can specify:

* The hostname on which a SnappyData locator is started.

* The startup directory where the logs and configuration files for that locator instance are located.

* SnappyData specific properties that can be passed.

You can refer to the **conf/locators.template** file for some examples.

### List of Locator Properties

Refer to the [SnappyData properties](property_description.md) for the complete list of SnappyData properties.

|Property|Description|
|-----|-----|
|-bind-address|IP address on which the locator is bound. The default behavior is to bind to all local addresses.|
|-classpath|Location of user classes required by the SnappyData Server.</br>This path is appended to the current classpath.|
|-client-port| The port that the network controller listens for client connections in the range of 1 to 65535. The default value is 1527.|
|-dir|The working directory of the server that contains the SnappyData Server status file and the default location for the log file, persistent files, data dictionary, and so forth (defaults to the current directory).|
|-heap-size|<a id="heap-size"></a> Sets the maximum heap size for the Java VM, using SnappyData default resource manager settings. </br>For example, -heap-size=1024m. </br>If you use the `-heap-size` option, by default SnappyData sets the critical-heap-percentage to 95% of the heap size, and the `eviction-heap-percentage` to 85.5% of the `critical-heap-percentage`. </br>SnappyData also sets resource management properties for eviction and garbage collection if the JVM supports them.|
|-J|JVM option passed to the spawned SnappyData server JVM. </br>For example, use -J-Xmx1024m to set the JVM heap to 1GB.|
|-J-Dsnappydata.enable-rls|Enables the system for row level security when set to true.  By default, this is off. If this property is set to true, then the Smart Connector access to SnappyData fails.|
|-locators|List of locators as comma-separated host:port values used to communicate with running locators in the system and thus discover other peers of the distributed system. </br>The list must include all locators in use and must be configured consistently for every member of the distributed system.|
|-log-file|Path of the file to which this member writes log messages. The default is **snappylocator.log** in the working directory. In case logging is set via log4j, the default log file is **snappydata.log**.|
|-member-timeout<a id="member-timeout"></a>|Uses the [member-timeout](../best_practices/important_settings.md#member-timeout) server configuration, specified in milliseconds, to detect the abnormal termination of members. The configuration setting is used in two ways:</br> 1) First, it is used during the UDP heartbeat detection process. When a member detects that a heartbeat datagram is missing from the member that it is monitoring after the time interval of 2 * the value of member-timeout, the detecting member attempts to form a TCP/IP stream-socket connection with the monitored member as described in the next case.</br> 2) The property is then used again during the TCP/IP stream-socket connection. If the suspected process does not respond to the are you alive datagram within the period specified in member-timeout, the membership coordinator sends out a new membership view that notes the member's failure. </br>Valid values are in the range 1000..600000.|
|-peer-discovery-address|Use this as value for the port in the "host:port" value of "-locators" property |
|-peer-discovery-port|The port on which the locator listens for peer discovery (includes servers as well as other locators).  </br>Valid values are in the range 1-65535, with a default of 10334.|
|Properties for SSL Encryption|[ssl-enabled](../reference/configuration_parameters/ssl_enabled.md), [ssl-ciphers](../reference/configuration_parameters/ssl_ciphers.md), [ssl-protocols](../reference/configuration_parameters/ssl_protocols.md), [ssl-require-authentication](../reference/configuration_parameters/ssl_require_auth.md).|

<a id="locator-example"></a>
**Example**: To start two locators on node-a:9999 and node-b:8888, update the configuration file as follows:

```pre
$ cat conf/locators
node-a -peer-discovery-port=9999 -dir=/node-a/locator1 -heap-size=1024m -locators=node-b:8888
node-b -peer-discovery-port=8888 -dir=/node-b/locator2 -heap-size=1024m -locators=node-a:9999
```

<a id="lead"></a>
## Configuring Leads

Lead Nodes primarily runs the SnappyData managed Spark driver. There is one primary lead node at any given instance, but there can be multiple secondary lead node instances on standby for fault tolerance. Applications can run Jobs using the REST service provided by the Lead node. Most of the SQL queries are automatically routed to the Lead to be planned and executed through a scheduler. You can refer to the **conf/leads.template** file for some examples. 

Create the configuration file (**leads**) for leads in the **<_SnappyData_home_>/conf** directory.

!!! Note
	In the **conf/spark-env.sh** file set the `SPARK_PUBLIC_DNS` property to the public DNS name of the lead node. This enables the Member Logs to be displayed correctly to users accessing SnappyData Monitoring Console from outside the network.

### List of Lead Properties
Refer to the [SnappyData properties](property_description.md) for the complete list of SnappyData properties.

|Property|Description</br>|
|-|-|
|-bind-address|IP address on which the lead is bound. The default behavior is to bind to all local addresses.|
|-classpath|Location of user classes required by the SnappyData Server.</br>This path is appended to the current classpath.|
|-critical-heap-percentage|Sets the Resource Manager's critical heap threshold in percentage of the old generation heap, 0-100. </br>If you set `-heap-size`, the default value for `critical-heap-percentage` is set to 95% of the heap size. </br>Use this switch to override the default.</br>When this limit is breached, the system starts canceling memory-intensive queries, throws low memory exceptions for new SQL statements, and so forth, to avoid running out of memory.|
|-dir|The working directory of the lead that contains the SnappyData Lead status file and the default location for the log file, persistent files, data dictionary, and so forth (defaults to the current directory).|
|-eviction-heap-percentage|Sets the memory usage percentage threshold (0-100) that the Resource Manager uses to evict data from the heap. By default, the eviction threshold is 85.5% of whatever is set for `-critical-heap-percentage`.</br>Use this switch to override the default.</br>|
|-heap-size|<a id="heap-size"></a> Sets the maximum heap size for the Java VM, using SnappyData default resource manager settings. </br>For example, `-heap-size=8g` </br> It is recommended to allocate minimum 6-8 GB of heap size per lead node. If you use the `-heap-size` option, by default SnappyData sets the critical-heap-percentage to 95% of the heap size, and the `eviction-heap-percentage` to 85.5% of the `critical-heap-percentage`. </br>SnappyData also sets resource management properties for eviction and garbage collection if the JVM supports them. |
|-J|JVM option passed to the spawned SnappyData Lead JVM. </br>For example, use -J-Xmx1024m to set the JVM heap to 1GB.|
|-J-Dsnappydata.enable-rls|Enables the system for row level security when set to true.  By default, this is off. If this property is set to true,  then the Smart Connector access to SnappyData fails.|
|-J-Dsnappydata.RESTRICT_TABLE_CREATION|Applicable when security is enabled in the cluster. If true, users cannot execute queries (including DDLs and DMLs) even in their default or own schema unless cluster admin explicitly grants them the required permissions using GRANT command. The default is false. |
|jobserver.waitForInitialization|When this property is set to true, the cluster startup waits for the Spark jobserver to be fully initialized before marking the lead node as **RUNNING**. The default is false.|
|-locators|List of locators as comma-separated host:port values used to communicate with running locators in the system and thus discover other peers of the distributed system. </br>The list must include all locators in use and must be configured consistently for every member of the distributed system.|
|-log-file|Path of the file to which this member writes log messages. The default **snappyleader.log** in the working directory. In case logging is set via log4j, the default log file is **snappydata.log**.|
|-member-timeout<a id="member-timeout"></a>|Uses the [member-timeout](../best_practices/important_settings.md#member-timeout) configuration, specified in milliseconds, to detect the abnormal termination of members. The configuration setting is used in two ways:</br> 1) First, it is used during the UDP heartbeat detection process. When a member detects that a heartbeat datagram is missing from the member that it is monitoring after the time interval of 2 * the value of member-timeout, the detecting member attempts to form a TCP/IP stream-socket connection with the monitored member as described in the next case.</br> 2) The property is then used again during the TCP/IP stream-socket connection. If the suspected process does not respond to the are you alive datagram within the period specified in member-timeout, the membership coordinator sends out a new membership view that notes the member's failure. </br>Valid values are in the range 1000..600000.|
|-memory-size|<a id="memory-size"></a>Specifies the total memory that can be used by the node for column storage and execution in off-heap. However, lead member do not need off-heap memory.  You can configure the off-heap memory for leads only when you are planning to increase the broadcast limit to a large value. This is generally not recommended and you must preferably limit the broadcast to a smaller value. The default off-heap size for leads is 0.|
|-snappydata.column.batchSize|The default size of blocks to use for storage in the SnappyData column store. The default value is 24M.|
|snappydata.hiveServer.enabled|Enables the Hive Thrift server for SnappyData. This is enabled by default when you start the cluster. Thus it adds an additional 10 seconds to the cluster startup time. To avoid this additional time, you can set the property to false.|
|spark.context-settings.num-cpu-cores| The number of cores that can be allocated. The default is 4. |
|spark.context-settings.memory-per-node| The executor memory per node (-Xmx style. For example: 512m, 1G). The default is 512m. |
|spark.context-settings.streaming.batch_interval| The batch interval for Spark Streaming contexts in milliseconds. The default is 1000.|
|spark.context-settings.streaming.stopGracefully| If set to true, the streaming stops gracefully by waiting for the completion of processing of all the received data. The default is true.|
|spark.context-settings.streaming.stopSparkContext| if set to true, the SparkContext is stopped along with the StreamingContext. The default is true.|
|-spark.driver.maxResultSize|Limit of the total size of serialized results of all partitions for each action (for example, collect). The value should be at least 1MB or 0 for unlimited. Jobs are aborted if the total size of the results is above this limit. Having a high limit may cause out-of-memory errors in the lead. The default max size is 1GB|
|-spark.executor.cores|The number of cores to use on each server.|
|-spark.jobserver.port|The port on which to run the jobserver. Default port is 8090.|
|-spark.jobserver.bind-address|The address on which the jobserver listens. Default address is 0.0.0.|
|-spark.jobserver.job-result-cache-size|The number of job results to keep per JobResultActor/context. The default is 5000.|
|-spark.jobserver.max-jobs-per-context|The number of jobs that can be run simultaneously in the context. The default is 8.|
|-spark.local.dir|Directory to use for "scratch" space in SnappyData, including map output files and RDDs that get stored on disk. This should be on a fast, local disk in your system. It can also be a comma-separated list of multiple directories on different disks.|
|-spark.network.timeout|The default timeout for all network interactions while running queries. |
|-spark.sql.codegen.cacheSize<a id="codegencache"></a>|Size of the generated code cache that is used by Spark, in the  SnappyData Spark distribution, and by SnappyData. The default is 2000.|
|-spark.ssl.enabled<a id="ssl_spark_enabled"></a>|Enables or disables Spark layer encryption. The default is false.|
|-spark.ssl.keyPassword<a id="ssl_spark_password"></a>|The password to the private key in the key store.|
|-spark.ssl.keyStore<a id="ssl_spark_keystore"></a>|Path to the key store file. The path can be absolute or relative to the directory in which the process is started.|
|-spark.ssl.keyStorePassword<a id="ssl_spark_keystorpass"></a>|The password used to access the keystore. |Lead|
|-spark.ssl.trustStore<a id="ssl_spark_trustore"></a>|Path to the trust store file. The path can be absolute or relative to the directory in which the process is started.|
|-spark.ssl.trustStorePassword<a id="truststorepassword"></a>|The password used to access the truststore.|
|-spark.ssl.protocol<a id="ssl_spark_ssl_protocol"></a>|The protocol that must be supported by JVM. For example, TLS.|
|-spark.ui.port|Port for your SnappyData Monitoring Console, which shows tables, memory and workload data. The default is 5050.|
|Properties for SSL Encryption|[ssl-enabled](../reference/configuration_parameters/ssl_enabled.md), [ssl-ciphers](../reference/configuration_parameters/ssl_ciphers.md), [ssl-protocols](../reference/configuration_parameters/ssl_protocols.md), [ssl-require-authentication](../reference/configuration_parameters/ssl_require_auth.md). </br> These properties need not be added to  the Lead members in case of a client-server connection.|

**Example**: To start a lead (node-l), set `spark.executor.cores` as 10 on all servers, and change the Spark UI port from 5050 to 9090, update the configuration file as follows:

```pre
$ cat conf/leads
node-l -heap-size=4096m -spark.ui.port=9090 -locators=node-b:8888,node-a:9999 -spark.executor.cores=10
```

<a id="confsecondarylead"></a>
###Configuring Secondary Lead

To configure secondary leads, you must add the required number of entries in the **conf/leads** file. 

For example:

```
$ cat conf/leads
node-l1 -heap-size=4096m -locators=node-b:8888,node-a:9999
node-l2 -heap-size=4096m -locators=node-b:8888,node-a:9999

```
In this example, two leads (one on node-l1 and another on node-l2) are configured. Using `sbin/snappy-start-all.sh`, when you launch the cluster, one of them becomes the primary lead and the other becomes the secondary lead.


<a id="dataserver"></a>
## Configuring Data Servers
Data Servers hosts data, embeds a Spark executor, and also contains a SQL engine capable of executing certain queries independently and more efficiently than the Spark engine. Data servers use intelligent query routing to either execute the query directly on the node or to pass it to the lead node for execution by Spark SQL. You can refer to the **conf/servers.template** file for some examples. 

Create the configuration file (**servers**) for data servers in the **<_SnappyData_home_>/conf** directory. 

### List of Server Properties
Refer to the [SnappyData properties](property_description.md) for the complete list of SnappyData properties.

|Property|Description</br>|
|-|-|
|-bind-address|IP address on which the server is bound. The default behavior is to bind to all local addresses.|
|-classpath|Location of user classes required by the SnappyData Server.</br>This path is appended to the current classpath.|
|-client-port| The port that the network controller listens for client connections in the range of 1 to 65535. The default value is 1527.|
|-critical-heap-percentage|Sets the Resource Manager's critical heap threshold in percentage of the old generation heap, 0-100. </br>If you set `-heap-size`, the default value for `critical-heap-percentage` is set to 95% of the heap size. </br>Use this switch to override the default.</br>When this limit is breached, the system starts canceling memory-intensive queries, throws low memory exceptions for new SQL statements, and so forth, to avoid running out of memory.|
|-critical-off-heap-percentage|Sets the critical threshold for off-heap memory usage in percentage, 0-100. </br>When this limit is breached, the system starts canceling memory-intensive queries, throws low memory exceptions for new SQL statements, and so forth, to avoid running out of off-heap memory.
|-dir|The working directory of the server that contains the SnappyData Server status file and the default location for the log file, persistent files, data dictionary, and so forth (defaults to the current directory). **work** is the default current working directory. |
|-eviction-heap-percentage|Sets the memory usage percentage threshold (0-100) that the Resource Manager will use to start evicting data from the heap. By default, the eviction threshold is 85.5% of whatever is set for `-critical-heap-percentage`.</br>Use this switch to override the default.</br>|
|-eviction-off-heap-percentage|Sets the off-heap memory usage percentage threshold, 0-100, that the Resource Manager uses to start evicting data from off-heap memory. </br>By default, the eviction threshold is 85.5% of the value that is set for `-critical-off-heap-percentage`. </br>Use this switch to override the default.|
|-heap-size|<a id="heap-size"></a> Sets the maximum heap size for the Java VM, using SnappyData default resource manager settings. </br>For example, -heap-size=1024m. </br>If you use the `-heap-size` option, by default SnappyData sets the critical-heap-percentage to 95% of the heap size, and the `eviction-heap-percentage` to 85.5% of the `critical-heap-percentage`. </br>SnappyData also sets resource management properties for eviction and garbage collection if the JVM supports them. |
|-memory-size|<a id="memory-size"></a>Specifies the total memory that can be used by the node for column storage and execution in off-heap. The default value is either 0 or it gets auto-configured in [specific scenarios](../configuring_cluster/configuring_cluster.md#autoconfigur_offheap).|
|-J|JVM option passed to the spawned SnappyData server JVM. </br>For example, use **-J-XX:+PrintGCDetails** to print the GC details in JVM logs.|
|-J-Dgemfirexd.hostname-for-clients|The IP address or host name that this server/locator sends to the JDBC/ODBC/thrift clients to use for the connection. The default value causes the `client-bind-address` to be given to clients. </br> This value can be different from `client-bind-address` for cases where the servers/locators are behind a NAT firewall (AWS for example) where `client-bind-address` needs to be a private one that gets exposed to clients outside the firewall as a different public address specified by this property. In many cases, this is handled by the hostname translation itself, that is, the hostname used in `client-bind-address` resolves to the internal IP address from inside and to the public IP address from outside, but for other cases, this property is required|
|-J-Dsnappydata.enable-rls|Enables the system for row level security when set to true.  By default, this is off. If this property is set to true,  then the Smart Connector access to SnappyData fails.|
|-J-Dsnappydata.RESTRICT_TABLE_CREATION|Applicable when security is enabled in the cluster. If true, users cannot execute queries (including DDLs and DMLs) even in their default or own schema unless cluster admin explicitly grants them the required permissions using GRANT command. The default is false.|
|-locators|List of locators as comma-separated host:port values used to communicate with running locators in the system and thus discover other peers of the distributed system. </br>The list must include all locators in use and must be configured consistently for every member of the distributed system.|
|-log-file|Path of the file to which this member writes log messages. The default is **snappyserver.log** in the working directory. In case logging is set via log4j, the default log file is **snappydata.log**.|
|-member-timeout<a id="member-timeout"></a>|Uses the [member-timeout](../best_practices/important_settings.md#member-timeout) server configuration, specified in milliseconds, to detect the abnormal termination of members. The configuration setting is used in two ways:</br> 1) First, it is used during the UDP heartbeat detection process. When a member detects that a heartbeat datagram is missing from the member that it is monitoring after the time interval of 2 * the value of member-timeout, the detecting member attempts to form a TCP/IP stream-socket connection with the monitored member as described in the next case.</br> 2) The property is then used again during the TCP/IP stream-socket connection. If the suspected process does not respond to the are you alive datagram within the period specified in member-timeout, the membership coordinator sends out a new membership view that notes the member's failure. </br>Valid values are in the range 1000..600000.|
|-rebalance<a id="rebalance"></a>|Causes the new member to trigger a rebalancing operation for all partitioned tables in the system. </br>The system always tries to satisfy the redundancy of all partitioned tables on new member startup regardless of this option. Usually rebalancing is triggered when the overall capacity is increased or reduced through member startup, shut down, or failure.|
|-spark.local.dir|Directory to use for "scratch" space in SnappyData, including map output files and RDDs that get stored on disk. This should be on a fast, local disk in your system. It can also be a comma-separated list of multiple directories on different disks.|
|Properties for SSL Encryption|[ssl-enabled](../reference/configuration_parameters/ssl_enabled.md), [ssl-ciphers](../reference/configuration_parameters/ssl_ciphers.md), [ssl-protocols](../reference/configuration_parameters/ssl_protocols.md), [ssl-require-authentication](../reference/configuration_parameters/ssl_require_auth.md).|
|-thrift-ssl<a id="thrift-properties"></a>|Specifies if you want to enable or disable SSL. Values: true or false|
|-thrift-ssl-properties|Comma-separated SSL properties including:</br>`protocol`: default "TLS",</br>`enabled-protocols`: enabled protocols separated by ":"</br>`cipher-suites`: enabled cipher suites separated by ":"</br>`client-auth`=(true or false): if client also needs to be authenticated </br>`keystore`: path to key store file </br>`keystore-type`: the type of key-store (default "JKS") </br>`keystore-password`: password for the key store file</br>`keymanager-type`: the type of key manager factory </br>`truststore`: path to trust store file</br>`truststore-type`: the type of trust-store (default "JKS")</br>`truststore-password`: password for the trust store file </br>`trustmanager-type`: the type of trust manager factory </br> |


**Example**: To start a two servers (node-c and node-c), update the configuration file as follows:

```pre
$ cat conf/servers
node-c -dir=/node-c/server1 -heap-size=4096m -memory-size=16g -locators=node-b:8888,node-a:9999
node-c -dir=/node-c/server2 -heap-size=4096m -memory-size=16g -locators=node-b:8888,node-a:9999
```
## Specifying Configuration Properties using Environment Variables

SnappyData configuration properties can be specified using environment variables LOCATOR_STARTUP_OPTIONS, SERVER_STARTUP_OPTIONS, and LEAD_STARTUP_OPTIONS respectively for locators, leads and servers.  These environment variables are useful to specify common properties for locators, servers, and leads.  These startup environment variables can be specified in **conf/spark-env.sh** file. This file is sourced when SnappyData system is started. A template file **conf/spark-env.sh.template** is provided in **conf** directory for reference. You can copy this file and use it to configure properties. 

For example:
```pre
# create a spark-env.sh from the template file
$cp conf/spark-env.sh.template conf/spark-env.sh 

# Following example configuration can be added to spark-env.sh, 
# it shows how to add security configuration using the environment variables

SECURITY_ARGS="-auth-provider=LDAP -J-Dgemfirexd.auth-ldap-server=ldap://192.168.1.162:389/ -user=user1 -password=password123 -J-Dgemfirexd.auth-ldap-search-base=cn=sales-group,ou=sales,dc=example,dc=com -J-Dgemfirexd.auth-ldap-search-dn=cn=admin,dc=example,dc=com -J-Dgemfirexd.auth-ldap-search-pw=password123"

#applies the configuration specified by SECURITY_ARGS to all locators
LOCATOR_STARTUP_OPTIONS=”$SECURITY_ARGS”
#applies the configuration specified by SECURITY_ARGS to all servers
SERVER_STARTUP_OPTIONS=”$SECURITY_ARGS”
#applies the configuration specified by SECURITY_ARGS to all leads
LEAD_STARTUP_OPTIONS=”$SECURITY_ARGS”


```
<a id="configure-smart-connector"></a>
## Configuring SnappyData Smart Connector  

Spark applications run as independent sets of processes on a cluster, coordinated by the SparkContext object in your main program (called the driver program). In Smart connector mode, a Spark application connects to SnappyData cluster to store and process data. SnappyData currently works with Spark version 2.1.1. To work with SnappyData cluster, a Spark application must set the `snappydata.connection` property while starting.   

| Property |Description |
|--------|--------|
| snappydata.connection        |SnappyData cluster's locator host and JDBC client port on which locator listens for connections. Has to be specified while starting a Spark application.|

**Example**:

```pre
$ ./bin/spark-submit --deploy-mode cluster --class somePackage.someClass  
	--master spark://localhost:7077 --conf spark.snappydata.connection=localhost:1527 
	--packages 'SnappyDataInc:snappydata:1.0.2.1-s_2.11'
```
<a id="environment"></a>
## Environment Settings

Any Spark or SnappyData specific environment settings can be done by creating a **snappy-env.sh** or **spark-env.sh** in **SNAPPY_HOME/conf**.

<a id="hadoop-setting"></a>
### Hadoop Provided Settings

If you want to run SnappyData with an already existing custom Hadoop cluster like MapR or Cloudera you should download Snappy without Hadoop from the download link. This allows you to provide Hadoop at runtime.

To do this, you need to put an entry in $SNAPPY-HOME/conf/spark-env.sh as below:

```pre
export SPARK_DIST_CLASSPATH=$($OTHER_HADOOP_HOME/bin/hadoop classpath)
```

<a id="logging"></a>
## Logging 

Currently, log files for SnappyData components go inside the working directory. To change the log file directory, you can specify a property _-log-file_ as the path of the directory. </br>
The logging levels can be modified by adding a *conf/log4j.properties* file in the product directory. 

```pre
$ cat conf/log4j.properties 
log4j.logger.org.apache.spark.scheduler.DAGScheduler=DEBUG
log4j.logger.org.apache.spark.scheduler.TaskSetManager=DEBUG
```
!!! Note
	For a set of applicable class names and default values see the file **conf/log4j.properties.template**, which can be used as a starting point. Consult the [log4j 1.2.x documentation](http://logging.apache.org/log4j/) for more details on the configuration file.

<a id="autoconfigur_offheap"></a>
## Auto-Configuring Off-Heap Memory Size

Off-Heap memory size is auto-configured by default in the following scenarios:

*	**When the lead, locator, and server are setup on different host machines:**</br>
	In this case, off-heap memory size is configured by default for the host machines with the server setup. The total size of heap and off-heap memory does not exceed more than 75% of the total RAM. For example, if the RAM is greater than 8GB, the heap memory is between 4-8 GB and the remaining becomes the off-heap memory.
    

* **When leads and one of the server node are on the same host:**</br>
In this case,  off-heap memory size is configured by default and is adjusted based on the number of leads that are present. The total size of heap and off-heap memory does not exceed more than 75% of the total RAM. However, here the heap memory is the total heap size of the server as well as that of the lead. 

!!! Note
	The off-heap memory size is not auto-configured when the heap memory and the off-heap memory are explicitly configured through properties or when multiple servers are on the same host machine. 

