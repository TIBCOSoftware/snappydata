# SYS.DISKSTORE_FSYNC

Flush and fsync all data to configured disk stores, including data in asynchronous persistence queues.

Use this procedure to ensure that all in-memory data is written to configured disk stores. You may want to fsync data in disk stores before copying or backing up disk store files at the operating system level, to ensure that they represent the current state of the SnappyData data.

## Syntax

```pre
SYS.diskstore_fsync (
IN DISKSTORENAME VARCHAR(128)
)
```

**DISKSTORENAME**   
The name of the disk store, as specified in the [CREATE DISKSTORE](../../reference/sql_reference/create-diskstore.md) statement. (Default or system-generated disk store names can be queried from the [SYSDISKSTORES](../../reference/system_tables/sysdiskstores.md) table.)

##Example

**Performs an fsync of the the data dictionary disk store file**:

```pre
snappy> call sys.diskstore_fsync('GFXD-DD-DISKSTORE');
```


