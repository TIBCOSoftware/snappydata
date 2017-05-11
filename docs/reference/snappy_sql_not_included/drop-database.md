#Drop Database

```
DROP (DATABASE|SCHEMA) [IF EXISTS] db_name [(RESTRICT|CASCADE)]
```

Drop a database. If the database to drop does not exist, an exception will be thrown. This also deletes the directory associated with the database from the file system.

**IF EXISTS**
If the database to drop does not exist, nothing will happen.

**RESTRICT**
Dropping a non-empty database will trigger an exception. Enabled by default

**CASCADE**
Dropping a non-empty database will also drop all associated tables and functions.