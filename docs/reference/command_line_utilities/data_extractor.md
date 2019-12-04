# Recovering Data During Cluster Failures
In scenarios where the TIBCO ComputeDB cluster fails to come up due to some issues, the Data Extractor utility can be used to retrieve the data in a standard format along with the schema definitions.

Typically, the TIBCO ComputeDB cluster starts when all the instances of the servers, leads, and locators within the cluster are started. However, sometimes the cluster does not come up and in such situations, there is a possibility that the data inside the cluster remains either completely or partially unavailable.
In such situations, you must first refer to the [Troubleshooting section](../troubleshooting/troubleshooting.md) in the TIBCO ComputeDB product documentation, fix the corresponding issues, and bring up the cluster. Even after this, if the cluster cannot be started successfully, due to unforeseen circumstances, you can use the Data Extractor utility to start the cluster in Recovery mode and salvage the data.

Data Extractor utility is a read-only mode of the cluster. In this mode, you cannot make any changes to the data such as INSERT, UPDATE, DELETE etc. Moreover, the inter-dependencies between the nodes during the startup process is minimized in this mode thereby reducing the chances of failures during startup. A cluster thus started in a minimalistic mode is called the Recovery mode.

In the Recovery mode:
*	You cannot perform operations with Data Definition Language (DDL) and Data Manipulation Language (DML).
*	You are provided with procedures to extract data, DDLs etc.
*	You can launch the Snappy shell and run SELECT/SHOW/DESCRIBE queries.

## Extracting Data in Recovery Mode

To bring up the cluster and salvage the data, do the following:
*	[Launch the cluster in a Recovery mode](#launchclusterrecovery)
*	[Retrieve all tables, table definitions, specified tables, and data DDL](#retrievedata)
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

After the cluster is brought into recovery mode, you can retrieve the metadata and the table data in the cluster. The following two system procedures are provided for this purpose:

*	`DUMP_DDLS`
*	`EXPORT_DATA`

Thus the table definitions and tables that are defined in a specific format can be exported and used later to launch a new cluster. 

!!!Caution
	Ensure to provide enough disk space, that is double the existing cluster size, to store recovered data.

#### Export Table Definitions in Text format
You can use the DUMP_DDLS system procedure to export table definitions in text format.  The DUMP_DDLS system procedure takes a single argument `exportURI`. You can provide any spark supported URI such as s3, local path, or HDFS. All the cluster definitions like TABLE, VIEW, DATABASE, FUNCTION, DEPLOY, ALTER, UPDATE, GRANT are exported in a text format into the provided `exportURI` argument.

**Syntax**

```
call sys.DUMP_DDLS('<export path>');
```

**Examples**

```
call sys.DUMP_DDLS('/home/xyz/extracted/ddls');
```

**Folder** **Structure**

```
ls /home/xyz/extracted/ddls_1571059691610/
part-00000  _SUCCESS
```

#### Export Tables Defined in a Specific Format

You can use the EXPORT_DATA system procedure to export the tables that are defined in a specified format into the provided path.

The EXPORT_DATA procedure accepts the following arguments:

| Arguments | Description |
|--------|--------|
|    exportURI    |     Spark supported URI to export data.|
|   formatType     |   Spark supported formats.     |
|  tableNames      | Comma-separated table names or all to dump all tables.|
| ignoreError       |   Ignores error while reading a table and moves on to reading the next table.|

**Syntax**

```
call sys.EXPORT_DATA('<exportURI>', '<formatType>', '<tableNames>', '<ignoreError>');
```

**Examples**

```
call sys.EXPORT_DATA('/home/xyz/extracted/data/', 'csv', 'all', 'true');
```

**Folder Structure**

```
ls /home/xyz/extracted/data_1571059786952/
APP.CT/  APP.RT/  APP.RTP/
ls /home/xyz/extracted/data_1571059786952/APP.CT/
part-00000-e6923433-5638-46ce-a719-b203c8968c88.csv  part-00001-e6923433-5638-46ce-a719-b203c8968c88.csv
```

<a id= loadextractdata> </a>
### Loading a New Cluster with Extracted Data
After the DDLs and data are extracted from the cluster, do the following to load a new cluster with the extracted data:
1.	Verify that the export path contains the table DDLs and the data by listing the output directories.
2.	Clear or move the work directory of the old cluster.
3.	Start a new cluster. 
4.	Use the dumped DDLs and data to create new definitions and repopulate the cluster.

## Limitations

The Data Extractor utility tries to recover data from redundant copies if one of the copies is corrupt but if no redundancy is available and the oplogs are corrupt, the utility fails to recover data.


