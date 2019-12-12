# Recovering Data During Cluster Failures
In scenarios where the TIBCO ComputeDB cluster fails to come up due to some issues, the Data Extractor utility can be used to retrieve the data in a standard format along with the schema definitions.

Typically, the TIBCO ComputeDB cluster starts when all the instances of the servers, leads, and locators within the cluster are started. However, sometimes, the cluster does not come up. In such situations, there is a possibility that the data inside the cluster remains either entirely or partially unavailable.
In such situations, you must first refer to the [Troubleshooting Common Problems](/troubleshooting/troubleshooting.md) section in the TIBCO ComputeDB product documentation, fix the corresponding issues, and bring up the cluster. Even after this, if the cluster cannot be started successfully, due to unforeseen circumstances, you can use the Data Extractor utility to start the cluster in Recovery mode and salvage the data.

Data Extractor utility is a read-only mode of the cluster. In this mode, you cannot make any changes to the data such as INSERT, UPDATE, DELETE, etc. Moreover, in this mode, the inter-dependencies between the nodes during the startup process is minimized. Therefore, there is a reduction in the chances of failures during startup. 

In the Recovery mode:

*	You cannot perform operations with Data Definition Language (DDL) and Data Manipulation Language (DML).
*	You are provided with procedures to extract data, DDLs, etc., and generate load-scripts.
*	You can launch the Snappy shell and run SELECT/SHOW/DESCRIBE queries.

## Extracting Data in Recovery Mode

To bring up the cluster and salvage the data, do the following:

1.	[Launch the cluster in a Recovery mode](#launchclusterrecovery)
2.	[Retrieve table definitions and data](#retrievedata)
3.	[Load a new cluster with data extracted from Recovery mode](#loadextractdata)

<a id= launchclusterrecovery> </a>
### Launching a Cluster in Recovery Mode

Launching a cluster in Recovery mode is similar to launching it in the regular mode. To specify this mode, all one has to do is pass an extra argument `-r` or `--recovery` to the cluster start script as shown in the following example:

```
snappy-start-all.sh -r
```

!!!Caution
	* DDL or DML cannot be executed in a Recovery Mode.
	* Recovery mode does not repair the existing cluster.
<a id= retrievedata> </a>
### Retrieving Metadata and Table Data

After you bring the cluster into recovery mode, you can retrieve the metadata and the table data in the cluster. The following system procedures are provided for this purpose:

*	[EXPORT_DDLs](/reference/inbuilt_system_procedures/export_ddl.md)
*	[EXPORT_DATA](/reference/inbuilt_system_procedures/export_data.md)

Thus the table definitions and tables in a specific format can be exported and used later to launch a new cluster. 

!!!Caution
	*   Ensure to provide enough disk space that is double the existing cluster size to store recovered data..
    *   If the DDL statements that are used on the cluster have credentials, it will appear masked in the output of the procedure EXPORT_DDLS. You must replace it before using the file. For example: JDBC URL in external tables, Location URIs in table options.


!!!Note
	In the recovery mode, you must check the console, the logs of the server, and the lead for errors. Also, check for any tables that are skipped during the export process. In case of any failures, visit the [Known issues](https://docs.tibco.com/products/tibco-computedb-enterprise-edition-1-2-0) in the release notes or [Troubleshooting Common Problems](/troubleshooting/troubleshooting.md) section in the product documentation to resolve the failures.


<a id= loadextractdata> </a>
## Loading a New Cluster with Extracted Data

After you extract the DDLs and the data from the cluster, do the following to load a new cluster with the extracted data:

1.	Verify that the exportURI that is provided to `EXPORT_DATA` contains the table DDLs and the data by listing the output directories. Also, ensure that the directory contains one sub-directory each for the tables in the cluster and that these sub-directories are not empty. If a sub-directory corresponding to a table is empty, stop proceeding further, stop the cluster, and go through the logs to find if there are any skipped or failed tables. In such a case, you must refer to the troubleshooting section for any known fixes or workarounds.
2.	Clear or move the work directory of the old cluster only after you have verified that the logs do not report any failures for any tables and that you have ensured to complete step 1.
3.	Start a new cluster.
4.	Connect to Snappy shell. 
5.	Use the exported DDLs and helper load-scripts to load the extracted data into the new cluster. 
	
    **For example**
	
            snappy-sql> run ‘/home/xyz/extracted/ddls_1571059691610/part-00000’;
            snappy-sql> run ‘/home/xyz/extracted/data_1571059786952_load_scripts/part-00000’;
	
### Example of  Using Data Extractor Utililty 

Here is an example of using DataExtractor utility to salvage data from a faulty cluster or a healthy cluster. The following are the sequence of steps to follow:

1.	Start a fresh cluster and create a table that you can attempt to recover using this example. 
2.	Start the Snappy shell and run the following DDL and INSERT statements:

```
snappy-sql
TIBCO ComputeDB version 1.2-SNAPSHOT
    snappy-sql> connect client 'localhost:1527';
    snappy-sql> create table customers(cid integer, name string, phone varchar(20));
    snappy-sql> insert into customers values (0, 'customer1', '9988776655'), (0, 'customer1', '9988776654');
    snappy-sql> exit;

# If the faulty cluster is not stopped completely yet, then run the “snappy-stop-all.sh” 
and ensure that all instances of cluster services are stopped. YOu can confirm this by 
checking the output of “jps”.
    snappy-stop-all.sh 
    The SnappyData Leader on user1-dell(localhost-lead-1) has stopped.
    The SnappyData Server on user1-dell(localhost-server-1) has stopped.
    The SnappyData Locator on user1-dell(localhost-locator-1) has stopped.
    jps
    11684 Main
    14295 Jps

# Launch the cluster in recovery mode by running the command “snappy-start-all.sh -r” and 
check the console output and take a note of all the instances that could come up 
and those that fail to come up.
    snappy-start-all.sh  -r
    Logs generated in /home/user1/workspace/snappydata/build-artifacts/scala-2.11/snappy/work/localhost-locator-1/snappylocator.log
    SnappyData Locator pid: 14500 status: running
      Distributed system now has 1 members.
      Started Thrift locator (Compact Protocol) on: localhost/127.0.0.1[1527]
    Logs generated in /home/user1/workspace/snappydata/build-artifacts/scala-2.11/snappy/work/localhost-server-1/snappyserver.log
    SnappyData Server pid: 14726 status: running
      Distributed system now has 2 members.
      Started Thrift server (Compact Protocol) on: localhost/127.0.0.1[1528]
    Logs generated in /home/user1/workspace/snappydata/build-artifacts/scala-2.11/snappy/work/localhost-lead-1/snappyleader.log
    SnappyData Leader pid: 14940 status: running
      Distributed system now has 3 members.
      Starting hive thrift server (session=snappy)
      Starting job server on: 0.0.0.0[8090]
  
# If any of the members fail to come up, and if redundancy is not used, there 
could be a partial loss of data that is hosted on those particular members.
If the cluster comes up successfully, launch “snappy-sql” shell, then you 
can run select queries or provided procedures to export DDLs, Data of the cluster.
Run the procedure “EXPORT_DDLS” to export the definitions of tables, views, UDFs etc.
    snappy-sql
    TIBCO ComputeDB version 1.2-SNAPSHOT 
    snappy-sql>  connect client 'localhost:1527';
    snappy-sql> call sys.EXPORT_DDLS('/tmp/recovered/ddls');
    snappy-sql> exit;
    ls /tmp/recovered/ddls_1576074336371/
    part-00000  _SUCCESS
    cat /tmp/recovered/ddls_1576074336371/part-00000
    create table customers(cid integer, name string, phone varchar(20));
    Next, run the procedure “EXPORT_DATA” to export the data of selected tables to selected location.
    snappy-sql
    TIBCO ComputeDB version 1.2-SNAPSHOT 
    snappy-sql> connect client 'localhost:1527';
    snappy-sql> call sys.EXPORT_DATA('/tmp/recovered/data', 'csv', 'all', true);
    snappy-sql> exit;
    ls /tmp/recovered/data_1576074561789
    APP.CUSTOMERS
    ls /tmp/recovered/data_1576074561789/APP.CUSTOMERS/
    part-00000-5da7511d-e2ca-4fe5-9c97-23c8d1f52204.csv  _SUCCESS
    cat /tmp/recovered/data_1576074561789/APP.CUSTOMERS/part-00000-5da7511d-e2ca-4fe5-9c97-23c8d1f52204.csv 
    cid,name,phone
    0,customer1,9988776654
    0,customer1,9988776655
    ls /tmp/recovered/data_1576074561789_load_scripts/
    part-00000  _SUCCESS
    cat /tmp/recovered/data_1576074561789_load_scripts/part-00000 
    CREATE EXTERNAL TABLE temp_app_customers USING csv
    OPTIONS (PATH '/tmp/recovered/data_1576074561789//APP.CUSTOMERS',header 'true');
    INSERT OVERWRITE app.customers SELECT * FROM temp_app_customers;

# With both the DDLs and data exported you can use the helper scripts already 
generated by the procedure “EXPORT_DATA” to load data into a fresh cluster. 
For the sake of the example we will stop then clear the work directory of the 
existing cluster and launch the cluster in normal mode to simulate a fresh cluster. 
Then run the DDL script and the helper load script to load the extracted data 
back into the fresh cluster.
    snappy-stop-all.sh 
    The SnappyData Leader on user1-dell(localhost-lead-1) has stopped.
    The SnappyData Server on user1-dell(localhost-server-1) has stopped.
    The SnappyData Locator on user1-dell(localhost-locator-1) has stopped.
    Rm -rf /home/user1/workspace/snappydata/build-artifacts/scala-2.11/snappy/work/
    snappy-start-all.sh 
    Logs generated in /home/user1/workspace/snappydata/build-artifacts/scala-2.11/snappy/work/localhost-locator-1/snappylocator.log
    SnappyData Locator pid: 19887 status: running
      Distributed system now has 1 members.
      Started Thrift locator (Compact Protocol) on: localhost/127.0.0.1[1527]
    Logs generated in /home/user1/workspace/snappydata/build-artifacts/scala-2.11/snappy/work/localhost-server-1/snappyserver.log
    SnappyData Server pid: 20062 status: running
      Distributed system now has 2 members.
      Started Thrift server (Compact Protocol) on: localhost/127.0.0.1[1528]
    Logs generated in /home/user1/workspace/snappydata/build-artifacts/scala-2.11/snappy/work/localhost-lead-1/snappyleader.log
    SnappyData Leader pid: 20259 status: running
      Distributed system now has 3 members.
      Starting hive thrift server (session=snappy)
      Starting job server on: 0.0.0.0[8090]
    snappy-sql
    TIBCO ComputeDB version 1.2-SNAPSHOT 
    snappy-sql> connect client 'localhost:1527';
    snappy-sql> run '/tmp/recovered/ddls_1576074336371/part-00000';
    snappy-sql> create table customers(cid integer, name string, phone varchar(20));
    snappy-sql> run '/tmp/recovered/data_1576074561789_load_scripts/part-00000';
    snappy-sql> CREATE EXTERNAL TABLE temp_app_customers USING csv
    OPTIONS (PATH '/tmp/recovered/data_1576074561789//APP.CUSTOMERS',header 'true');
    snappy-sql> INSERT OVERWRITE app.customers SELECT * FROM temp_app_customers;
    2 rows inserted/updated/deleted
    show tables;
    schemaName                                                                        |tableName                                                                                |isTemporary
    ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    app                                                                               |customers                                                                                |false      
    app                                                                               |temp_app_customers                                                                       |false      

    2 rows selected
    snappy-sql> select * from customers;
    cid        |name           |phone               
    ------------------------------------------------
    0          |customer1      |9988776655          
    0          |customer1      |9988776654          

    2 rows selected



```

## Viewing the User Interface in Recovery Mode
In the recovery mode, by default, the table counts and sizes do not appear on the UI. To view the table counts, you should set the property **snappydata.recovery.enableTableCountInUI** to **true** in the lead's conf file. By default, the property is set to **false**, and the table count is shown as **-1**.

The cluster that you have started in the recovery mode with the flag, can get busy fetching the table counts for some time based on the data size. If the table counts are enabled and there is an error while reading a particular table, the count is shown as **-1**.

## Known Issues

*	If one of the copies is corrupt, the Data Extractor utility attempts to recover data from redundant copies. However, if the redundancy is not available, and if the data files are corrupt, the utility fails to recover data.
*	The Leader log contains an error message: ***Table/View 'DBS' does not exist***. You can ignore this message.

## Troubleshooting

*	Your TIBCO ComputeDB cluster has tables with big schema or a large number of buckets and this cluster has stopped without any exceptions in the log.</br>	
    **Workaround**: Add the property, `-recovery-state-chunk-size` into the conf files of each server, and set the value lower than the current(default 30). This property is responsible for chunking the table information while moving across the network in recovery mode.
	For example: `localhost -recovery-state-chunk-size=10` </br>	

*	An error message, ***Expected compute to launch at x but was launched at y*** is shown. </br>
    **Workaround**: Increase the value of the property `spark.locality.wait.process` to more than current value (default 1800s).
    The error could be due to any of the following reasons:
	*	The data distribution is skewed, which causes more tasks to be assigned to server **x** which further  leads to time-out for some tasks, and are re-launched on server **y**.
	*	The expected host is the best option to get the data; others are not in good shape.
	It is imperative to use a specific host for a particular partition( of a table) to get data in recovery mode reliably.</br>	
    
    You can also face this error when there was a failure on server **x** hence the task scheduler re-launches the task on server **y**, without waiting for the time-out. In this case the log for server **x** should be analysed for errors.
   
*	You are facing memory-related issues such as **LowMemoryException** or if the server gets killed and a `jvmkill_<pid>.log` is generated. In such a case enough memory may not be available to the servers.</br>	
	**Workaround**: Decrease the number of CPUs available for each server. This action ensures that at a time, less number of tasks are launched simultaneously. You can also decrease the cores by individually setting the `-spark.executor.cores` property to a lower value, in the server's conf file. After this, restart the cluster in recovery mode and again export the failed tables.