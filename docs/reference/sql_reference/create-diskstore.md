# CREATE DISKSTORE

Disk stores provide disk storage for tables that need to overflow or persist.

```pre
CREATE DISKSTORE diskstore_name

    [ MAXLOGSIZE max-log-size-in-mb ]
    [ AUTOCOMPACT boolean-constant ]
    [ ALLOWFORCECOMPACTION boolean-constant ]
    [ COMPACTIONTHRESHOLD garbage-threshold ]
    [ TIMEINTERVAL time-after-which-data-is-flused-to-disk ]
    [ WRITEBUFFERSIZE buffer-size-in-mb ]
    [ QUEUESIZE max-row-operations-to-disk ]
    [ ( 'dir-name' [ disk-space-in-mb ] [,'dir-name' [ disk-space-in-mb ] ]* ) ]
```

## Description

TIBCO ComputeDB attempts to preallocate oplog files when you execute the CREATE DISKSTORE command. 

<!-- See [Preventing Disk Full Errors](../../best_practices/prevent_disk_full_errors.md) for more information.
-->
All tables that target the same disk store share that disk store's persistence attributes. A table that does not target a named disk store uses the default disk store for overflow or persistence. By default, TIBCO ComputeDB uses the working directory of the member as the default disk store.


**MAXLOGSIZE**

TIBCO ComputeDB records DML statements in an operation log (oplog) files. This option sets the maximum size in megabytes that the oplog can become before TIBCO ComputeDB automatically rolls to a new file. This size is the combined sizes of the oplog files. When TIBCO ComputeDB creates an oplog file, it immediately reserves this amount of file space. TIBCO ComputeDB only truncates the unused space on a clean shutdown (for example, `snappy server stop` or `./sbin/snappy-stop-all`).

The default value is 1 GB.

**AUTOCOMPACT**

Set this option to "true" (the default) to automatically compact disk files. Set the option to "false" if compaction is not needed or if you intend to manually compact disk files using the `snappy` utility.

TIBCO ComputeDB performs compaction by removing "garbage" data that DML statements generate in the oplog file.

**ALLOWFORCECOMPACTION**

Set this option to "true" to enable online compaction of oplog files using the `snappy` utility. By default, this option is set to "false" (disabled).

**COMPACTIONTHRESHOLD**

Sets the threshold for the amount of "garbage" data that can exist in the oplog before TIBCO ComputeDB initiates automatic compaction. Garbage data is created as DML operations create, update, and delete rows in a table. The threshold is defined as a percentage (an integer from 0–100). The default is 50. When the amount of "garbage" data exceeds this percentage, the disk store becomes eligible for auto-compaction if AUTOCOMPACT is enabled.

**TIMEINTERVAL**

Sets the number of milliseconds that can elapse before TIBCO ComputeDB asynchronously flushes data to disk. TIMEINTERVAL is only used for tables that were created using the `asynchronous` option in the persistence clause of the CREATE TABLE statement. See [CREATE TABLE](create-table.md). The default value is 1000 milliseconds (1 second).

**WRITEBUFFERSIZE**

Sets the buffer size in bytes to use when persisting data to disk. The default is 32768 bytes.

**QUEUESIZE**

Sets the maximum number of row operations that TIBCO ComputeDB asynchronously queues to disk. After this number of asynchronous operations are queued, additional asynchronous operations block until existing writes are flushed to disk. A single DML operation may affect multiple rows, and each row modification, insertion, and deletion are considered a separate operation. The default QUEUESIZE value is 0, which specifies no limit.

**dir-name**

The optional `dir-name` entry defines a specific host system directory to use for the disk store. You can include one or more `dir-name` entries using the syntax:

```pre
[ ( 'dir-name' [ disk-space-in-mb ] [,'dir-name' [ disk-space-in-mb ] ]* ) ]
```

In each entry:

* `dir-name` specifies the name of a directory to use for the disk store. The disk store directory is created on each member if necessary. If you do not specify an absolute path, then TIBCO ComputeDB creates or uses the named directory in each member's working directory (or in the value specified by the [sys-disk-dir](../../reference/configuration_parameters/sys-disk-dir.md) boot property, if defined). If you specify an absolute path, then all parent directories in the path must exist at the time you execute the command.

	!!! Note 
    	TIBCO ComputeDB uses a "shared nothing" disk store design, and you cannot use a single disk store directory to store oplog files from multiple TIBCO ComputeDB members. 

*   *disk-space-in-mb* optionally specifies the maximum amount of space, in megabytes, to use for the disk store in that directory. The space used is calculated as the combined sizes of all oplog files in the directory.

    If you do not specify the *disk-space-in-mb* value, then TIBCO ComputeDB does not impose a limit on the amount of space used by disk store files in that directory. If you do specify a limit, the size must be large enough to accommodate the disk store oplog files (the `MAXLOGSIZE` value, or 1 GB by default) and leave enough free space in the directory to avoid low disk space warnings. If you specify a size that cannot accommodate the oplog files and maintain enough free space, TIBCO ComputeDB fails to create the disk store with SQLState error XOZ33: Cannot create oplogs with size {0}MB which is greater than the maximum size {1}MB for store directory ''{2}''.

You can specify any number of `dir-name` entries in a `CREATE DISKSTORE` statement. The default diskstore does not have an option for multiple directories. It is recommended that you use separate data area from the server working directory.
Create a separate diskstore and specify multiple directories. The data is spread evenly among the active disk files in the directories, keeping within any limits you set.

## Example

This example uses the default base directory and parameter values to create a named disk store:

```pre
snappy> CREATE DISKSTORE STORE1;
```

This example configures disk store parameters and specifies a storage directory:

```pre
snappy> CREATE DISKSTORE STORE1
      MAXLOGSIZE 1024 
      AUTOCOMPACT TRUE
      ALLOWFORCECOMPACTION  FALSE 
      COMPACTIONTHRESHOLD  80
      TIMEINTERVAL  223344
      WRITEBUFFERSIZE 19292393
      QUEUESIZE 17374
      ('dir1' 10240);
```

This example specifies multiple storage directories and directory sizes for oplog files:

```pre
snappy> CREATE DISKSTORE STORE1 
      WRITEBUFFERSIZE 19292393
      QUEUESIZE 17374
      ('dir1' 456 , 'dir2', 'dir3' 532 );
```

**Related Topics**

* [DROP DISKSTORE](drop-diskstore.md)

* [SYSDISKSTORES](../system_tables/sysdiskstores.md)
