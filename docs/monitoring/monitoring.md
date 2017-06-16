# Overview
SnappyData Dashboard is a monitoring system that gives you a high-level overview of the status and performance of the cluster. It provides a simple widget based view which helps you easily navigate and monitor your cluster.</br>

To access the SnappyData Dashboard, start your cluster and open http:`<leadhost>`:5050/dashboard/ in your web browser. </br>
`<leadhost>` is the hostname or IP of the lead node in your cluster.

The Dashboard also displays the **Last Updated Date** and **Time of statistics** on the top-left side of the page.


## The Dashboard
SnappyData Dashboard offers the following capabilities and benefits:

### Clusters Statistics

* **Cluster Status**</br>
	Displays the current status of the cluster. 
    
    | Status | Description |
	|--------|--------|
	|**Normal**|All nodes in the cluster are running|
    |**Warning **|Some nodes in the cluster are stopped or unavailable|

* **CPU Usage** </br>
   Displays the average CPU utilization of all the nodes present in the cluster.

* **Memory Usage**</br>
   Displays the collective usage of on-heap and off-heap memory by all nodes in the cluster.

* **JVM Heap Usage**</br>
   Displays the collective JVM Heap usage by all nodes in the cluster.

### Members Statistics

* **Members Count**</br>
   Displays the total number of members (leads, locators and data servers) that exist in the cluster. The tooltip displays the count for each member.

* **Members status**</br>
   Displays the status of the members, which can be either Running or Stopped.

* **Description**</br>
  	A brief description of the member is displayed in Member column. You can view the detailed description for the member by clicking on the arrow next to the member name.</br>
  	The description provides details of the member host, working directory, and process ID.

* **Type**</br>
   Displays the type of member, which can be lead, locator or data server.

* **CPU Usage**</br>
   The CPU utilized by the member's host.

* **Memory Usage**</br>
   Members collective Heap and Off-Heap Memory utilization along with Total Memory.

* **Heap Memory**</br>
   Displays the total available heap memory and the used heap memory.</br> 
   You can view the detailed description of the member's heap storage, heap execution memory, utilizations along with JVM Heap utilization by clicking on the arrow next to the member name.

* **Off-Heap Memory Usage**</br>
   Displays the members total off-heap memory and used off-heap memory.</br> You can also view the member's off-heap storage and off-heap execution memory and utilizations by clicking on the arrow next to the member name.

### Tables Statistics

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