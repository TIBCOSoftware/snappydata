#Insert

```
INSERT INTO [TABLE] [db_name.]table_name [PARTITION part_spec] select_statement

INSERT OVERWRITE TABLE [db_name.]table_name [PARTITION part_spec] select_statement

part_spec:
    : (part_col_name1=val1, part_col_name2=val2, ...)
```

Insert data into a table or a partition using a select statement.

**OVERWRITE**
Whether to override existing data in the table or the partition. If this flag is not provided, the new data is appended.