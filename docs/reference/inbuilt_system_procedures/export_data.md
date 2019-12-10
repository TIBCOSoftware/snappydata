# SYS.EXPORT_DATA

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

```
