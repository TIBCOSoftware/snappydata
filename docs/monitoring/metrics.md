# Monitoring with Metrics

Metrics constitutes of the measurements of resource usage or behavior that can be observed and collected all over TIBCO ComputeDB clusters. Using the Metrics feature, you can monitor the cluster health and statistics.  Monitoring the clusters allows you to do the following:

*	Increase availability by quickly detecting downtime/degradation.
*	Facilitate performance monitoring by external tools. These external tools can handle functions such as metrics aggregation, alerting, and visualization.

TIBCO ComputeDB uses the [Spark’s Metrics Subsystem](https://spark.apache.org/docs/latest/monitoring.html#metrics) for metrics collection. This system allows you to publish the metrics to a variety of sinks that you can enable for metrics collection. By default, the Metrics servlet sink is enabled.  
Spark supports the following sinks for Metrics. TIBCO ComputeDB can send metrics to all these sinks. However, it is recommended to use the **MetricsServlet** for exploratory purposes.

| Sink | Description |
|--------|--------|
|   MetricsServlet     |  Adds a servlet within the existing Spark UI to serve metrics data as JSON data. </br>This is used for exploratory analysis and is easily accessible via a web page. </br>The servlet URL is http://<leadhost>:5050/metrics/json.        |
|      JmxSink  |     Register metrics for viewing in a JMX console. Monitoring tools/ agents that understand the JMX protocol, such as JMX exporter from Prometheus, can export this information to external monitoring systems for storage and visualization.   |
|   CsvSink     |   Exports metrics data to CSV files at regular intervals.     |
|      ConsoleSink  |   Logs metrics information to the console.    |
|      GraphiteSink  |     Sends metrics to a Graphite node.   |
|  Slf4jSink      |    Sends metrics to slf4j as log entries.    |
|      StatsdSink  |  Sends metrics to a StatsD node.|

!!!Note
	You cannot store metrics for long term retention as well as for archival purposes. Detailed instructions for integration with external monitoring systems to build monitoring dashboards by sourcing metrics data from the sinks will be published in a future release.

Whenever you start the cluster, the metrics are published and made available through commonly used sinks, which can be consumed by monitoring tools. In TIBCO ComputeDB, you can publish the metrics using the collector service on the Lead node. Metrics are not published for Smart Connector mode. 

The following type of metrics are made available for TIBCO ComputeDB:

*	**Availability Metrics**</br>
	This type of metrics alerts about the status and availability of the cluster. 
	
    Examples:
    
	*	Cluster and node status
	*	Cluster and node uptime/downtime

*	**Performance Metrics**</br>
	This type of metric provides insights into system performance. 

	Examples:
    
    *	Throughput measured as transactions per second / Queries served per hour (TPS/ QpH)
    *	The total number of queries served
    *	Number of jobs running/completed
    *	Resource usage for system-level resources such as CPU, memory, disk, and network.
    *	Resource usage for ComputeDB application/component resources such as heap or off-heap memory and their percentage utilization etc.

## Enabling the Sinks for Metrics collection

If  you want to publish the TIBCO ComputeDB metrics in any of the sinks, you must enable these sinks in the **metrics.properties** file, which is located in the **conf** folder of the TIBCO ComputeDB installation directory. 

To enable a sink, do the following:

1.	Open the **conf** folder and change the name of **metrics.properties.template** to  **metrics.properties** file. 
	
    	cp metrics.properties.template metrics.properties
    
2.	Uncomment the properties of the Sink that you want to enable and provide the necessary property values. 
	For example, for enabling ConsoleSink, you must uncomment the following properties: 
    
		#Enable ConsoleSink for all instances by class name
		#*.sink.console.class=org.apache.spark.metrics.sink.ConsoleSink
		# Polling period for the ConsoleSink
		#*.sink.console.period=10
		# Unit of the polling period for the ConsoleSink
		#*.sink.console.unit=seconds
		# Polling period for the ConsoleSink specific for the master instance
		#master.sink.console.period=15
		# Unit of the polling period for the ConsoleSink specific for the master instance
		#master.sink.console.unit=seconds
		

3.	Start the cluster for the configurations to take effect by executing this command from the TIBCO ComputeDB installation folder:

		./sbin/snappy-start-all.sh`


!!!Note
	In case you make changes to **metrics.properties** file when the cluster is running, you must always restart the cluster for the configuration changes to reflect. 

## Accessing Metrics

You can check the TIBCO ComputeDB metrics collected through MetricsServlet at the following URL:
`<Lead node hostname>:<5050>/metrics/json`

The default sink for metrics monitoring is **MetricServlet** and the metrics for TIBCO ComputeDB are available at `http://(lead-hostname):5050/metrics/json`.

### Accessing Metrics from JmxSink 
Do the following to access Metrics from JmxSink:

1.	Enable **org.apache.spark.metrics.sink.JmxSink** in the metrics configuration file. You can then use **JConsole** to access Metrics through JMX.
 
		*.sink.jmx.class=org.apache.spark.metrics.sink.JmxSink

3.	Launch **JConsole**, select TIBCO ComputeDB primary lead process. 
3.	Go to **MBeans** > **metrics**, to access the TIBCO ComputeDB Metrics. For remote processes, you need to add the following JMX remote properties in the node configuration file.

        -jmx-manager=true -jmx-manager-start=true 
        -jmx-manager-port=<port_value>
	

### Accessing Metrics from CsvSink 
Do the following to access Metrics from CsvSink:

1.	Enable **org.apache.spark.metrics.sink.CsvSink** in metrics configuration file.

    	*.sink.csv.class=org.apache.spark.metrics.sink.CsvSink
 
2.	You can now access the CSV files created for TIBCO ComputeDB statistics from the value specified in the ***.sink.csv.directory** property. Ensure that the directory already exists and has the write permissions.

## Disabling Metrics Collection

To disable the metrics collection for a specific Sink, edit the **metrics.properties** file and comment the corresponding entries by adding the prefix **#** to the required lines. Save the file and then restart the cluster. 


## Statistics

Using the Metrics feature, you can collect the following statistics in TIBCO  ComputeDB.

### TableCountStatistics

| **Source** | **Description** |**Metric Type** | **Probable Values** |
|--------|--------|--------|--------|
|     embeddedTablesCount   |    Count of the embedded tables.    |     Gauge  |        |
|  row tables      |     Count of the row tables.   |     Gauge  |        |
|    columnTablesCount    |   Count of the column tables.     |   Gauge    |        |
|   externalTablesCount     | Count of external tables.     |   Gauge    |        |


### Table Statistics

| **Source** | **Description** |**Metric** **Type** | **Probable** **Values** |
|--------|--------|--------|--------|
|    tableName    |      For each table, the tableName is provided as the fully qualified table name.   |  Gauge     |  |
|  isColumnTable      | Specifies if it is a column table.     |  Gauge     |   Boolean value(True or False).     |
|    rowCount    |   Number of rows.     |    Gauge   |    Specifies the number of rows inserted which is otherwise 0.  |
|   sizeInMemory     |  Table size in memory.      |    Gauge   |        |
|  sizeSpillToDisk      |     Table size spilled to disk.   |    Gauge   |        |
|   totalSize     |   Total size of the table.     |    Gauge   |        |
|     isReplicatedTable   |      Specifies if it is a replicated table.  |   Gauge    |    Boolean Values(True or False)|
|      bucketCount  |    Number of buckets.    |Gauge     |        |
|   redundancy     |  Specifies if the redundancy is enabled.      |  Gauge     |    Specifies the redundancy value provided while creating a table which is otherwise 0.    |
|     isRedundancyImpaired   |   Specifies if the redundancy is impaired. (since one or more replicas are unavailable)     |   Gauge    |   Boolean Values(True or False)     |
|     isAnyBucketLost   |  Specifies if any buckets are lost. (UI shows bucket count in red color.)      |     Gauge  |Boolean Values(True or False)      |

### ExternalTableStatistics 

| **Source** | **Description** | **Metric Type** | **Probable Values** |
| --- | --- | --- | --- |
| dataSourcePath | Data source path. | Gauge | Path of the file from which data to be loaded |
| provider | Data source provider. | Gauge | csv, parquet, orc, json, etc |
| tableName | Table name | Gauge |   |
| tableType | Table type | Gauge | EXTERNAL |

### MemberStatistics

| **Source** | **Description** | **Metric Type** | **Probable Values** |
| --- | --- | --- | --- |
| totalMembersCount | Count of total members. | Gauge |   |
| leadCount | Count of leads. | Gauge |   |
| locatorCount | Count of locators. | Gauge |   |
| dataServerCount | Count of data servers. | Gauge |   |
| connectorCount | Count of connectors. | Gauge |   |

### MemberStatistics
(Histograms are published in MB units)

| **Source** | **Description** | **Metric Type** | **Probable Values** |
| --- | --- | --- | --- |
| memberId | Contains IP address, port and process ID. | Gauge |   |
| nameOrId | Contains IP address, port, and pid or the name | Gauge |   |
| host | IP address or name of the machine. | Gauge |   |
| shortDirName | Relative path of the log directory. | Gauge |   |
| fullDirName | Absolute path of log directory. | Gauge |   |
| logFile | Name of the log file. | Gauge |   |
| processId | Member&#39;s process ID. | Gauge |   |
| diskStoreUUID | Member&#39;s unique disk store UUID. | Gauge |   |
| diskStoreName | Member&#39;s disk store name. | Gauge |   |
| status | Current status of the member. | Gauge | Running / Stopped |
| memberType | Type (Lead/ Server/ Locator/ Accessor). | Gauge | Lead / Locator/ Data Server |
| isLocator | Flag returns true if the member is locator or false otherwise. | Gauge | Boolean value(True or false) |
| isDataServer | Flag returns true  if the  member is data server or false otherwise. | Gauge | Boolean value(True or false) |
| isLead | Flag returns true if the member is lead or false otherwise. | Gauge | Boolean value(True or false) |
| isActiveLead | Flag returns true if the member is primary lead or false otherwise. | Gauge | Boolean value(True or false) |
| cores | Total number of cores. | Gauge |   |
| cpuActive | Number of active CPUs. | Gauge |   |
| clients | Number of connected clients connected | Gauge |   |
| jvmHeapMax | Max JVM heap size. | Gauge |   |
| jvmHeapUsed | Used JVM heap size. | Gauge |   |
| jvmHeapTotal | Total JVM heap size. | Gauge |   |
| jvmHeapFree | Free JVM heap size. | Gauge |   |
| heapStoragePoolUsed | Used heap storage pool. | Gauge |   |
| heapStoragePoolSize | Heap storage pool size. | Gauge |   |
| heapExecutionPoolUsed | Used heap execution pool. | Gauge |   |
| heapExecutionPoolSize | Heap execution pool size. | Gauge |   |
| heapMemorySize | Heap memory size. | Gauge |   |
| heapMemoryUsed | Used heap memory. | Gauge |   |
| offHeapStoragePoolUsed | Used off-heap storage pool. | Gauge |   |
| offHeapStoragePoolSize | Off-heap storage pool size. | Gauge |   |
| offHeapExecutionPoolUsed | Used off-heap execution pool. | Gauge |   |
| offHeapExecutionPoolSize | Off-heap execution pool size. | Gauge |   |
| offHeapMemorySize | Off-heap memory size. | Gauge |   |
| offHeapMemoryUsed | Used off-heap memory. | Gauge |   |
| diskStoreDiskSpace | Disk store disk space. | Gauge |   |
| cpuUsage | CPU usage. | Gauge |   |
| jvmUsage | JVM usage. | Gauge |   |
| heapUsage | Heap usage. | Gauge |   |
| heapStorageUsage | Heap storage usage. | Gauge |   |
| heapExecutionUsage | Heap execution usage. | Gauge |   |
| offHeapUsage | Off-heap usage. | Gauge |   |
| offHeapStorageUsage | Off-heap storage usage. | Gauge |   |
| offHeapExecutionUsage | Off-heap  execution size | Gauge |   |
| aggrMemoryUsage | Aggregate memory usage. | Gauge |   |
| cpuUsageTrends | CPU usage trends. | Histogram |   |
| jvmUsageTrends | JVM usage trends.  | Histogram |   |
| heapUsageTrends | Heap usage trends. | Histogram |   |
| heapStorageUsageTrends | Heap storage usage trends. | Histogram |   |
| heapExecutionUsageTrends | Heap execution usage trends. | Histogram |   |
| offHeapUsageTrends | Off-heap usage trends.  | Histogram |   |
| offHeapStorageUsageTrends | Off-heap storage usage trends. | Histogram |   |
| offHeapExecutionUsageTrends | Off-heap execution usage trends.  | Histogram |   |
| aggrMemoryUsageTrends | Aggregate memory usage trends.  | Histogram |   |
| diskStoreDiskSpaceTrend | Disk store and space trends. | Histogram |   |

###ClusterStatistics
(Histograms are published in MB units)

| **Source** | **Description** | **Metric Type** | **Probable Values** |
| --- | --- | --- | --- |
| totalCores | Totals number of cores in the cluster.  | Gauge |   |
| jvmUsageTrends | JVM usage trends in the cluster.  | Histogram |   |
| heapUsageTrends | Heap usage trends in the cluster.  | Histogram |   |
| heapStorageUsageTrends | JVM usage trends in the cluster.  | Histogram |   |
| heapExecutionUsageTrends | Heap execution usage trends in the cluster.  | Histogram |   |
| offHeapUsageTrends | Off-heap usage trends in the cluster.  | Histogram |   |
| offHeapStorageUsageTrends | Off-heap storage usage trends in the cluster.  | Histogram |   |
| offHeapExecutionUsageTrends | Off-heap execution usage trends in the cluster.  | Histogram |   |
| aggrMemoryUsageTrends | Aggregate memory usage trends in the cluster.  | Histogram |   |
| diskStoreDiskSpaceTrend | Disk store and space trends in the cluster.  | Histogram |   |


