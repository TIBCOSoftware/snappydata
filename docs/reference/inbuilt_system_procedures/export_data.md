# SYS.EXPORT_DATA

You can use the EXPORT_DATA system procedure to export the tables in a specified format into the provided path. The procedure also generates helper scripts which you can use to load the extracted data into a new cluster. Verify there are non-empty directories for the respective tables for all the tables shown on the  UI. After the data is loaded into the new cluster successfully, drop all the external tables. 



The EXPORT_DATA procedure accepts the following arguments:

| Arguments | Description |
|--------|--------|
|    exportURI    |  Specify the Spark supported URI to export data. For example S3, HDFS, NFS, Local FileSystem etc.|
|   formatType     | Specify Spark supported formats. For example CSV, Parquet, JSON etc.|
|  tableNames      | Provide comma-separated table names `(<schema>.<tablename>)` or specify `all` to export all the tables.|
| ignoreError       | Specify this setting as **true** to ignore errors while reading a table and move on to reading the next table. Any errors that occur while exporting a table is logged and the next table is exported.  When a table is skipped due to failures, no data is available, and you can retry exporting it after troubleshooting. You must also check both the lead as well as the server logs for failures.|

!!!Note
	The directory created is of the pattern `<exportURI_TimeInMillis>`.

## Syntax

```
call sys.EXPORT_DATA('<exportURI>', '<formatType>', '<tableNames>', '<ignoreError>');
```

# Examples

```
call sys.EXPORT_DATA('/home/xyz/extracted/data/', 'csv', 'all', 'true');
call sys.EXPORT_DATA('/home/xyz/extracted/data/', 'parquet', 'CT,RT', 'false');

```

## Folder Structure

```
ls /home/xyz/extracted/data_1571059786952/
APP.CT/  APP.RT/  APP.RTP/

ls /home/xyz/extracted/data_1571059786952/APP.CT/
part-00000-e6923433-5638-46ce-a719-b203c8968c88.csv  part-00001-e6923433-5638-46ce-a719-b203c8968c88.csv

ls /home/xyz/extracted/data_1571059786952_load_scripts/
part-00000  _SUCCESS

Using generated load scripts
run ‘/home/xyz/extracted/data_1571059786952_load_scripts/part-00000’;

```


For statistics search the leader log for the following message:

```
Successfully exported <N>  tables.
Exported tables are: <TableNames>
Failed to export <N> tables.
Failed tables are <TableNames>
```

