# SYS.DISKSTORE_FSYNC

Flush and fsync all data to configured disk stores, including data in asynchronous persistence queues.

Use this procedure to ensure that all in-memory data is written to configured disk stores. You may want to fsync data in disk stores before copying or backing up disk store files at the operating system level, to ensure that they represent the current state of the SnappyData data.

##Syntax

``` pre
SYS.diskstore_fsync (
IN DISKSTORENAME VARCHAR(128)
)
```

**DISKSTORENAME**   
The name of the disk store, as specified in the <a href="../language_ref/ref-create-diskstore.html#create-diskstore" class="xref noPageCitation" title="Disk stores provide disk storage for tables and queues that need to overflow or persist (for instance when using an asynchronous write-behind listener).">CREATE DISKSTORE</a> statement. (Default or system-generated disk store names can be queried from the <a href="../system_tables/sysdiskstores.html#reference_36E65EC061C34FB696529ECA8ABC5BFC" class="xref noPageCitation" title="Contains information about all disk stores created in the SnappyData distributed system.">SYSDISKSTORES</a> table.)

##Example

**Performs an fsync of the the data dictionary disk store file**:

``` pre
snappy> call sys.diskstore_fsync('GFXD-DD-DISKSTORE');
Statement executed.
```


