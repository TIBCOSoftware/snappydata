# Configuring, Launching a SnappyData Cluster

Before you configure the SnappyData cluster, check the [system requirements](/install/system_requirements.md). In case you have not yet provisioned SnappyData, you can follow the instructions [here](/install.md).  
TIBCO recommends that you have at least **8 GB** of memory and **4 cores** available even for simple experimentation with SnappyData.


## Launch Cluster with Default Configuration
If you want to launch the cluster either on Amazon EC2 or on a Kubernetes cluster, you can follow the instructions listed [here (AWS)](/install/setting_up_cluster_on_amazon_web_services.md) and [here (Kubernetes)](/kubernetes.md)

If you are launching on a single node, for example, on your laptop or on-premise server, you can do so using this simple command:

```
./sbin/snappy-start-all.sh 
```
This launches a single [locator](configuring_cluster.md#locator), [lead](configuring_cluster.md#lead) and a [data server](configuring_cluster.md#dataserver). You can go to the following URL on your browser to view the cluster dashboard:

**http://(localhost or hostname or machineIP):5050** 

By default, the cluster uses the following resources:

| Cluster Component | Port |Memory Used|
|--------|--------||
| Lead       |**5050** (http port used for dashboard.) </br>**8090** (Port used to submit Spark streaming or batch jobs.) </br>**10000** (Port for hive thrift server.)        |**4 GB**|
|    Locator    |  **1527** (Port used by JDBC clients.)</br> **10334** (Ports used for all cluster members to communicate with each other.)        |**1 GB**|
|Server |**1528** (Port used by ODBC or JDBC clients)  |**4 GB**|


All the artifacts created such as the server - logs, metrics, and the database files are all stored in a folder called **work** in the product home directory. Click the individual member URLs on the dashboard to view the logs.

**Also see**:

*	[Connecting with JDBC](/howto/connect_using_jdbc_driver.md)

*	[Connecting with ODBC](/howto/connect_using_odbc_driver.md) 

*	[Submitting a Spark Job](/howto/run_spark_job_inside_cluster.md)

<!---(notes: /tmp recommendation, decide on the folder where you want to persist the data ... sizing for this) --->