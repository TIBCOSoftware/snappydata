# SYS.EXPORT_DDLS

You can use the EXPORT_DDLs system procedure to export table definitions in text format.  The EXPORT_DDLs system procedure takes a single argument, `exportURI`. You can provide any spark supported URI such as s3, local path, or HDFS. All the DDLs such as TABLE, VIEW, DATABASE, FUNCTION, DEPLOY, ALTER, UPDATE, GRANT are exported to a text file in `exportURI` with the name **part-00000**. Verify there are respective DDLs in the generated file for all the tables shown on the UI. 

!!!Note
    The directory created is of the pattern `<exportURI_TimeInMillis>`.
    
## Syntax

```
call sys.EXPORT_DDLS('<exportURI>');
```

## Examples

```
call sys.EXPORT_DDLS('/home/xyz/extracted/ddls');
```

## Folder Structure

```
ls /home/xyz/extracted/ddls_1571059691610/
part-00000  _SUCCESS

Reloading extracted DDLs
run ‘/home/xyz/extracted/ddls_1571059691610/part-00000’;

```


For statistics search the leader log for message `Successfully exported <N> DDL statements.`
