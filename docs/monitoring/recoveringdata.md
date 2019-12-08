# Recovering Data During Cluster Failures
In scenarios where the SnappyData cluster fails to come up due to some issues, the Data Extractor utility can be used to retrieve the data in a standard format along with the schema definitions.

Typically, the SnappyData cluster starts when all the instances of the servers, leads, and locators within the cluster are started. However, sometimes, the cluster does not come up. In such situations, there is a possibility that the data inside the cluster remains either entirely or partially unavailable.
In such situations, you must first refer to the **Troubleshooting Common Problems** section in the SnappyData product documentation, fix the corresponding issues, and bring up the cluster. Even after this, if the cluster cannot be started successfully, due to unforeseen circumstances, you can use the Data Extractor utility to start the cluster in Recovery mode and salvage the data.

Data Extractor utility is a read-only mode of the cluster. In this mode, you cannot make any changes to the data such as INSERT, UPDATE, DELETE, etc. Moreover, in this mode, the inter-dependencies between the nodes during the startup process is minimized. Therefore, there is a reduction in the chances of failures during startup. 

In the Recovery mode:
*	You cannot perform operations with Data Definition Language (DDL) and Data Manipulation Language (DML).
*	You are provided with procedures to extract data, DDLs, etc., and generate load-scripts.
*	You can launch the Snappy shell and run SELECT/SHOW/DESCRIBE queries.

## Extracting Data in Recovery Mode

To bring up the cluster and salvage the data, do the following:

*	[Launch the cluster in a Recovery mode](#launchclusterrecovery)
*	[Retrieve table definitions and data](#retrievedata)
*	[Load a new cluster with data extracted from Recovery mode](#loadextractdata)

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

*	`EXPORT_DDLs`
*	`EXPORT_DATA`

Thus the table definitions and tables in a specific format can be exported and used later to launch a new cluster. 

!!!Caution
	Ensure to provide enough disk space that is double the existing cluster size to store recovered data.

#### Export DDLs in Text Format
You can use the EXPORT_DDLs system procedure to export table definitions in text format.  The EXPORT_DDLs system procedure takes a single argument, `exportURI`. You can provide any spark supported URI such as s3, local path, or HDFS. All the DDLs such as TABLE, VIEW, DATABASE, FUNCTION, DEPLOY, ALTER, UPDATE, GRANT are exported to a text file in `exportURI` under the name **part-00000**.

!!!Note
    The directory created is of the pattern `<exportURI_TimeInMillis>`.

**Syntax**

```
call sys.EXPORT_DDLS('<exportURI>');
```

**Examples**

```
call sys.EXPORT_DDLS('/home/xyz/extracted/ddls');
```

**Folder** **Structure**
Workaround
```
ls /home/xyz/extracted/ddls_1571059691610/
part-00000  _SUCCESS

```

#### Export Tables in a Specific Format

You can use the EXPORT_DATA system procedure to export the tables in a specified format into the provided path. The procedure also generates helper scripts which you can use to load the extracted data into a new cluster.

The EXPORT_DATA procedure accepts the following arguments:

| Arguments | Description |
|--------|--------|
|    exportURI    |    Specify the Spark supported URI to export data.|
|   formatType     | Specify Spark supported formats. |
|  tableNames      | Provide comma-separated table names `(<schema>.<tablename>)` or specify `all` to export all the tables.|
| ignoreError       |  Specify this to ignore errors while reading a table and move on to reading the next table.|

!!!Note
	The directory created is of the pattern `<exportURI_TimeInMillis>`.

**Syntax**

```
call sys.EXPORT_DATA('<exportURI>', '<formatType>', '<tableNames>', '<ignoreError>');
```

**Examples**

```
call sys.EXPORT_DATA('/home/xyz/extracted/data/', 'csv', 'all', 'true');
call sys.EXPORT_DATA('/home/xyz/extracted/data/', 'parquet', 'CT,RT', 'false');

```

**Folder Structure**

```
ls /home/xyz/extracted/data_1571059786952/
APP.CT/  APP.RT/  APP.RTP/

ls /home/xyz/extracted/data_1571059786952/APP.CT/
part-00000-e6923433-5638-46ce-a719-b203c8968c88.csv  part-00001-e6923433-5638-46ce-a719-b203c8968c88.csv

ls /home/xyz/extracted/data_1571059786952_load_scripts/
part-00000  _SUCCESS

```

!!!Note
	You must check the console, the logs of the server, and the lead for errors. Also, check for any tables that are skipped during the export process. In case of any failures, visit the [Known issues](https://docs.tibco.com/products/tibco-computedb-enterprise-edition-1-2-0) in the release notes or **Troubleshooting Common Problems** section in the product documentation to resolve the failures.


<a id= loadextractdata> </a>
## Loading a New Cluster with Extracted Data

After you extract the DDLs and the data from the cluster, do the following to load a new cluster with the extracted data:

1.	List the output directories and verify that the exportURI that you have provided to `EXPORT_DATA`, contains the table DDLs. Also ensure that the directory contains one sub-directory each for the tables in the cluster and that these sub-directories are not empty. If a sub-directory corresponding to a table is empty, stop proceeding further, stop the cluster. Go through the logs to find if there are any skipped or failed tables. In such a case, you must refer to the troubleshooting section for any known fixes or workarounds.
2.	Clear or move the work directory of the old cluster only after you have verified that the logs do not report any failures for any tables and that you have ensured to complete step 1.
3.	Start a new cluster.
4.	Connect to Snappy shell. 
5.	Use the exported DDLs and helper load-scripts to load the extracted data into the new cluster. 
	
    **For example**
	
            snappy-sql> run ‘/home/xyz/extracted/ddls_1571059691610/part-00000’;
            snappy-sql> run ‘/home/xyz/extracted/data_1571059786952_load_scripts/part-00000’;
	

## Viewing the User Interface in Recovery Mode
In the recovery mode, by default, the table counts and sizes do not appear on the UI. To view the table counts, you should set the property **snappydata.recovery.enableTableCountInUI** to **true** in the lead's conf file. By default, the property is set to **false**, and the table count is shown as **-1**.

The cluster that you have started in the recovery mode with the flag, can get busy fetching the table counts for some time based on the data size. If the table counts are enabled and there is an error while reading a particular table, the count is shown as **-1**.

## Known Issues

*	If one of the copies is corrupt, the Data Extractor utility attempts to recover data from redundant copies. However,   if the redundancy is not available, and if the data files are corrupt, the utility fails to recover data.
*	The Leader log contains an error message: ***Table/View 'DBS' does not exist***. You can ignore this message.

## Troubleshooting

*	Your SnappyData cluster has tables with big schema or a large number of buckets and this cluster has stopped without any exceptions in the log.</br>	
    **Workaround**: Add the property, `-recovery-state-chunk-size` into the conf files of each server, and set the value lower than the current(default 30). This property is responsible for chunking the table information while moving across the network in recovery mode.
	For example: `localhost -recovery-state-chunk-size=10` </br>	

*	*An error message, ***Expected compute to launch at x but was launched at y*** is shown. </br>
    **Workaround**: Increase the value of the property `spark.locality.wait.process` to more than current value (default 1800s).
    The error could be due to any of the following reasons:
	*	The data distribution is skewed.
	*	The expected host is the best option to get the data; others are not in good shape.
	It is imperative to use a specific host for a particular partition( of a table) to get data in recovery mode reliably.</br>	
    
*	You are facing memory-related issues such as **LowMemoryException** or if the server gets killed and a `jvmkill_<pid>.log` is generated. In such a case enough memory may not be available to the servers.</br>	
	**Workaround**: Decrease the number of CPUs available for each server. This action ensures that at a time, less number of tasks are launched simultaneously. You can also decrease the cores by individually setting the `-spark.executor.cores` property to a lower value, in the server's conf file. After this, restart the cluster in recovery mode and again export the failed tables.

