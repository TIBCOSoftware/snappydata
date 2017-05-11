# Create Database


```
CREATE (DATABASE|SCHEMA) [IF NOT EXISTS] db_name
    [COMMENT comment_text]
    [LOCATION path]
    [WITH DBPROPERTIES (key1=val1, key2=val2, ...)]
```

Create a database. If a database with the same name already exists, an exception will be thrown.

**IF NOT EXISTS**
If a database with the same name already exists, nothing will happen.

**LOCATION**
If the specified path does not already exist in the underlying file system, this command will try to create a directory with the path. When the database is dropped later, this directory will be deleted.