# TRUNCATE TABLE

Remove all content from a table and return it to its initial, empty state. TRUNCATE TABLE clears all in-memory data for the specified table as well as any data that was persisted to RowStore disk stores. For HDFS read-write tables, TRUNCATE TABLE also marks the table's HDFS persistence files for expiiry. For HDFS write-only tables, TRUNCATE TABLE leaves all table data that is stored in HDFS log files available for later processing using MapReduce or HAWQ.

##Syntax

``` pre
TRUNCATE TABLE table-name
```

!!! Note:
	This command clears the in-memory table data from an HDFS-persistent table, even if you do not specify the `queryHDFS` hint.</br>
	Most DML commands (update and delete operations) always operate against the full data set of the table, even if you do not specify the `queryHDFS` hint. For HDFS Write-only tables, TRUNCATE TABLE removes only the in-memory data for the table, but leaves the HDFS log files available for later processing with MapReduce and HAWQ. This occurs regardless of the `queryHDFS` setting.
	</br>For HDFS Read-Write tables, TRUNCATE TABLE removes the in-memory data for the table and marks the table's HDFS log files for expiry. This means that RowStore queries, MapReduce jobs with CHECKPOINT mode enabled, and HAWQ external tables with CHECKPOINT mode enabled no longer return data for the truncated table. MapReduce jobs and HAWQ external tables that do not use CHECKPOINT mode will continue to return some table values until the log files expire.

<a id="reference_6CBB1645D7E74B0096C9F556AC754EBE__section_F731B973A8A6465DA0065687ABB5FA84"></a>
##Description

To truncate a table, you must be the table's owner. You cannot use this command to truncate system tables.

##Example

To truncate the "flights" table in the current schema:

``` pre
TRUNCATE TABLE flights;
```


