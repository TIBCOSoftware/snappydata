#Drop Table

```
DROP TABLE [IF EXISTS] [db_name.]table_name
```

Drop a table. If the table to drop does not exist, an exception will be thrown. This also deletes the directory associated with the table from the file system if this is not an EXTERNAL table.

**IF EXISTS**
If the table to drop does not exist, nothing will happen.