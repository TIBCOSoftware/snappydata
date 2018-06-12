# SnappyData Pulse

SnappyData Pulse is a monitoring system that gives you a high-level overview of the status and performance of the cluster. It provides a simple widget based view, which helps you easily navigate and monitor your cluster.</br>

To access the SnappyData Pulse, start your cluster and open http:`<leadhost>`:5050/dashboard/ in your web browser. </br>
`<leadhost>` is the hostname or IP of the lead node in your cluster.

![Dashboard](../Images/monitoring_topnav.png)

The top-right side of the page displays the date and time when the Dashboard was last updated. Click on the product version number to view details like the build number, source revision, underlying spark version, etc.

!!! Note:
	- When using Smart Connector with upstream Spark, the **Dashboard** and **Member Details** sections are not displayed. Only the **SQL**, **Jobs** and **Stages** related information is displayed.

The following topics are covered in this section:

* [Dashboard](#dashboard)

* [Member Details View](#member-details)

* [SQL Page](#sql)

* [Jobs Page](#jobs)

* [Stages Page](#stages)

<a id="dashboard"></a>
## The Dashboard

The Dashboard offers the following capabilities and benefits:

* [Cluster Statistics](#cluster)

* [Member Statistics](#member)

* [Table Statistics](#table)

* [External Table Statistics](#external-table)

<a id="cluster"></a>
### Cluster Statistics

![Cluster](../Images/monitoring_cluster.png)

* **Cluster Status**</br>
	Displays the current status of the cluster. 
    
    | Status | Description |
	|--------|--------|
	|**Normal**|All nodes in the cluster are running|
    |**Warning**|Some nodes in the cluster are stopped or unavailable|

* **CPU Usage** </br>
   Displays the average CPU utilization of all the nodes present in the cluster.

* **Memory Usage**</br>
   Displays the collective usage of on-heap and off-heap memory by all nodes in the cluster.

* **JVM Heap Usage**</br>
   Displays the collective JVM Heap usage by all nodes in the cluster.

<a id="member"></a>
### Member Statistics

![Dashboard](../Images/monitoring_member.png)

* **Members Count**</br>
   Displays the total number of members (leads, locators and data servers) that exist in the cluster. The tooltip displays the count for each member type.

* **Members Status**</br>
   Displays the status of the members, which can be either Running or Stopped.

| Status | Description |
|--------|--------|
|![Running](../Images/running-status.png)|Member is running|
|![Stopped](../Images/stopped-status.png)|Member has stopped or is unavailable|

* **Description**</br>
  	A brief description of the member is displayed in **Member** column. You can view the detailed description for the member by clicking on the arrow next to the member name.</br>
  	You can view details of the member by clicking on it. The description provides details of the member host, working directory, and process ID.

* **Type**</br>
   Displays the type of member, which can be lead, locator or data server. The active lead is displayed in bold.

* **CPU Usage**</br>
   The CPU utilized by the member's host.

* **Memory Usage**</br>
   Members collective Heap and Off-Heap Memory utilization along with Total Memory.

* **Heap Memory**</br>
   Displays the total available heap memory and used heap memory.</br> 
   You can view the detailed distribution of the member's heap storage, heap execution memory, their utilization along with JVM Heap utilization by clicking on the arrow next to the member name.

* **Off-Heap Memory Usage**</br>
   Displays the members total off-heap memory and used off-heap memory.</br> You can also view the member's off-heap storage and off-heap execution memory and utilization by clicking on the arrow next to the member name.

<a id="table"></a>
### Table Statistics

![Dashboard](../Images/monitoring_table.png)

* **Tables Count**</br>
   Displays the total number of data tables present in the cluster. The tooltip displays the count for the row and column tables.

* **Name**</br>
  Displays the name of the data table.

* **Storage Model**</br>
   Displays the data storage model of the data table. Possible models are ROW and COLUMN.

* **Distribution Type**</br>
   Displays the data distribution type for the table. Possible values are PARTITION, PARTITION_PERSISTENT, PARTITION_REDUNDANT, PARTITION_OVERFLOW, REPLICATE, REPLICATE_PERSISTENT, REPLICATE_OVERFLOW etc.

* **Row Count**</br>
   Displays the row count, which is the number of records present in the data table.

* **Memory Size**</br>
   Displays the heap memory used by data table to store its data/records.

* **Total Size**</br>
   Displays the collective physical memory and disk overflow space used by the data table to store its data/records.
<!--
 **BUCKETS**</br>
   Displays the total number of buckets in the data table.
-->

![Dashboard](../Images/monitoring_external_table.png)
<a id="external-table"></a>
### External Table Statistics

* **Tables Count**</br>
	Displays the total number of external tables present in the cluster. The tooltip displays the total number of external tables.

* **Name**</br>
	Displays the name of the external table.

* **Provider**</br>
	Displays the datastores provide used when creating the external table. For example, Parquet, CSV, JDBC etc.

* **Source**</br>
	For Parquet and CSV format, the path of the data file used to create the external table is displayed. For JDBC, the name of the client driver is displayed.

<a id="member-details"></a>
## Member Details View

The Member Details View offers the following capabilities and benefits:

* [Member Statistics](#memberstats)

* [Member Logs](#memberlogs)

The top-right side of the page displays the last updated date and time for the member.

<a id="memberstats"></a>
### Member Statistics

![Member Stats](../Images//monitoring_memberdetails_stats.png)

* **Member Name/ID**</br> Displays the name or ID of the member. 
	
* **Type**</br> Displays the type of member, which can be LEAD, LOCATOR or DATA SERVER.
	
* **Process ID**</br> Displays the process ID of the member. 

* **Heap Memory**</br> Displays the total available heap memory, used heap memory, their distribution into heap storage, heap execution memory and their utilization.

* **Off-Heap Memory Usage**</br>
   Displays the members total off-heap memory, used off-heap memory, their distribution into off-heap storage and off-heap execution memory, and their utilization.
	
* **Member Status**</br> Displays the current status of the member which can be either Running or Stopped.
   
    | Status | Description |
	|--------|--------|
	|![Running Member](../Images/running-status.png)|Member is running|
    |![Stopped Member](../Images/stopped-status.png)|Member has been stopped or is unavailable|

* **CPU Usage**</br> The CPU utilized by the member's host.

* **Memory Usage**</br> Members collective Heap and Off-Heap Memory utilization along with total memory.

* **Memory JVM Usage**</br> Members total JVM Heap and its utilization.

<a id="memberlogs"></a>
### Member Logs

![MemberLogs](../Images/monitoring_memberdetails_logs.png)

* **Log File Location**</br>Displays the absolute path of the member's primary log file on a host where the current member process is running. 
	
* **Log Details**</br>Displays details of the loaded logs like Loaded Bytes, Start and End Indexes of Loaded Bytes and Total Bytes of logs content.

* **Logs**</br>Displays the actual log entries from the log files. </br> It also displays two clickable buttons to load more log entries from log files.   </br>
	*  **Load New**: Loads latest log entries from the log file (if generated) after logs were last loaded/updated.    </br> 
	*  **Load More**: Loads older log entries from log files, if available.

<a id="sql"></a>
## SQL Page
![](../Images/query_analysis_sql.png)

* **Colocated**: When colocated tables are joined on the partitioning columns, the join happens locally on the node where data is present, without the need of shuffling the data. This improves the performance of the query significantly instead of broadcasting the data across all the data partitions.

* **Whole-Stage Code Generation**: A whole stage code generation node compiles a sub-tree of plans that support code generation together into a single Java function, which helps improve execution performance.

* **Per node execution timing**: Displays the time required for the execution of each node. If there are too many rows that are not getting filtered or exchanged.

* **Pool Name**: Default/Low Latency. Applications can explicitly configure the use of this pool using a SQL command `set snappydata.scheduler.pool=lowlatency`. 

* **Query Node Details**: Move the mouse over a component to view its details.

* **Filter**: Displays the number of rows that are filtered for each node. 

* **Joins**: If HashJoin puts pressure on memory, you can change the HashJoin size to use SortMergeJoin to avoid on-heap memory pressure.

<a id="jobs"></a>
## Jobs Page
![](../Images/query_analysis_job.png)

* **Status**: Displays the status of the job. 

* **Stages**: Click on the stage to view its details. The table displays the time taken for completion of each stage. 

!!! Tip:
	You can cancel a long running job, using the **Kill** option. </br>![kill](../Images/kill_job.png)

<a id="stages"></a>
## Stages Page
![](../Images/query_analysis_stage.png)

* On this page, you can view the total time required for all the tasks in a job to complete.

* You can view if any tasks have taken a long time to complete. This may occur in case of uneven data distribution. 

* Scheduler Delay indicates the waiting period for the task. Delays can  be caused if there are too many concurrent jobs.

* Shuffle reads and writes: Shuffles are written to disk and take a lot of time to write and read. This can be avoided by using colocated and replicated tables. You can use high-performance SSD drives for temporary storage (spark.local.dir) to improve shuffle time.  

* Number of parallel tasks: Due to concurrency, multiple queries may take cores and a particular query may take longer. To fix this, you can create a new scheduler and [assign appropriate cores to it](../best_practices/setup_cluster.md).

* GC time: Occasionally, on-heap object creation can slow down a query because of garbage collection. In these cases, it is recommended that you increase the on-heap memory (especially when you have row tables).
