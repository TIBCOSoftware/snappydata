# CREATE DISKSTORE

Disk stores provide disk storage for tables and queues that need to overflow or persist (for instance when using an asynchronous write-behind listener).

##Syntax

``` pre
CREATE DISKSTORE diskstore_name

    [ MAXLOGSIZE integer-constant ]
    [ AUTOCOMPACT boolean-constant ]
    [ ALLOWFORCECOMPACTION boolean-constant ]
    [ COMPACTIONTHRESHOLD integer-constant ]
    [ TIMEINTERVAL integer-constant ]
    [ WRITEBUFFERSIZE integer-constant ]
    [ QUEUESIZE integer-constant ]
    [ ( 'dir-name' [ integer-constant ] [,'dir-name' [ integer-constant ] ]* ) ]
```

<a id="create-diskstore__section_CEFE3C3073E342A6B431737BC51FC781"></a>
##Description

RowStore attempts to preallocate oplog files when you execute the CREATE DISKSTORE command. See <a href="../../troubleshooting.html#concept_83A53C7C767442578F2A4CF50E4A224E__section_a11_lxt_hl" class="xref">Preventing Disk Full Errors</a> for more information.

All tables that target the same disk store share that disk store's persistence attributes. A table that does not target a named disk store uses the default disk store for overflow or persistence. By default, RowStore uses the working directory of the member as the default disk store.

See also <a href="../../disk_storage/chapter_overview.html#disk_storage" class="xref" title="By default, a RowStore distributed system persists only the data dictionary for the tables and indexes you create. These persistence files are stored in the datadictionary subdirectory of each locator and data store that joins the distributed system. In-memory table data, however, is not persisted by default; if you shut down all RowStore members, non-persistent tables are empty on the next startup. You have the option to persist all table data to disk as a backup of the in-memory copy, or to overflow table data to disk when it is evicted from memory.">Persisting Table Data to RowStore Disk Stores</a>.

**MAXLOGSIZE**

RowStore records DML statements in an operation log (oplog) files. This option sets the maximum size in megabytes that the oplog can become before RowStore automatically rolls to a new file. This size is the combined sizes of the <span class="ph filepath">.crf</span> and <span class="ph filepath">.drf</span> oplog files. When RowStore creates an oplog file, it immediately reserves this amount of file space. RowStore only truncates the unused space on a clean shutdown (for example, `snappy-shell rowstore server stop` or `snappy-shell shut-down-all`).

The default value is 1 GB.

**AUTOCOMPACT**

Set this option to "true" (the default) to automatically compact disk files. Set the option to "false" if compaction is not needed or if you intend to manually compact disk files using the `snappy-shell` utility.

RowStore performs compaction by removing "garbage" data that DML statements generate in the oplog file.

**ALLOWFORCECOMPACTION**

Set this option to "true" to enable online compaction of oplog files using the `snappy-shell` utility. By default, this option is set to "false" (disabled).

**COMPACTIONTHRESHOLD**

Sets the threshold for the amount of "garbage" data that can exist in the oplog before RowStore initiates automatic compaction. Garbage data is created as DML operations create, update, and delete rows in a table. The threshold is defined as a percentage (an integer from 0–100). The default is 50. When the amount of "garbage" data exceeds this percentage, the disk store becomes eligible for auto-compaction if AUTOCOMPACT is enabled.

**TIMEINTERVAL**

Sets the number of milliseconds that can elapse before RowStore asynchronously flushes data to disk. TIMEINTERVAL is only used for tables that were created using the `asynchronous` option in the persistence clause of the CREATE TABLE statement. See <a href="ref-create-table.html#create-table" class="xref" title="Creates a new table using RowStore features.">CREATE TABLE</a>. The default value is 1000 milliseconds (1 second).

**WRITEBUFFERSIZE**

Sets the buffer size in bytes to use when persisting data to disk. The default is 32768 bytes.

**QUEUESIZE**

Sets the maximum number of row operations that RowStore asynchronously queues to disk. After this number of asynchronous operations are queued, additional asynchronous operations block until existing writes are flushed to disk. A single DML operation may affect multiple rows, and each row modification, insertion, and deletion is considered a separate operation. The default QUEUESIZE value is 0, which specifies no limit.

***dir-name***

The optional `dir-name` entry defines a specific host system directory to use for the disk store. You can include one or more `dir-name` entries using the syntax:

``` pre
[ ( 'dir-name' [ integer-constant ] [,'dir-name' [ integer-constant ] ]* ) ]
```

In each entry:

*   `dir-name` specifies the name of a directory to use for the disk store. The disk store directory is created on each member if necessary. If you do not specify an absolute path, then RowStore creates or uses the named directory in each member's working directory (or in the value specified by the <a href="../configuration/ConnectionAttributes.html#jdbc_connection_attributes__section_86AA2AD28CEB4C4E945434AC6564A4CC" class="xref noPageCitation">sys-disk-dir</a> boot property, if defined). If you specify an absolute path, then all parent directories in the path must exist at the time you execute the command.<p class="note"><strong>Note:</strong> RowStore uses a "shared nothing" disk store design, and you cannot use a single disk store directory to store oplog files from multiple RowStore members. </p>
*   *integer-constant* optionally specifies the maximum amount of space, in megabytes, to use for the disk store in that directory. The space used is calculated as the combined sizes of all oplog files in the directory.

    If you do not specify an *integer-constant* value, then RowStore does not impose a limit on the amount of space used by disk store files in that directory. If you do specify a limit, the size must be large enough to accommodate the disk store oplog files (the `MAXLOGSIZE` value, or 1 GB by default) and leave enough free space in the directory to avoid low disk space warnings (see <a href="../configuration/ConnectionAttributes.html#jdbc_connection_attributes__diskspace-warning-interval" class="xref">gemfire.DISKSPACE\_WARNING\_INTERVAL</a>). If you specify a size that cannot accommodate the oplog files and maintain enough free space, RowStore fails to create the disk store with SQLState error XOZ33: Cannot create oplogs with size {0}MB which is greater than the maximum size {1}MB for store directory ''{2}''.

You can specify any number of `dir-name` entries in a `CREATE DISKSTORE` statement. The data is spread evenly among the active disk files in the directories, keeping within any limits you set.

##Example

This example uses the default base directory and parameter values to create a named disk store:

``` pre
snappy> CREATE DISKSTORE STORE1;
```

This example configures disk store parameters and specifies a storage directory:

``` pre
snappy> CREATE DISKSTORE STORE1 
    MAXLOGSIZE 1024 
    AUTOCOMPACT TRUE
    ALLOWFORCECOMPACTION  FALSE 
    COMPACTIONTHRESHOLD  80
    TIMEINTERVAL  223344
    WRITEBUFFERSIZE 19292393
    QUEUESIZE 17374
    ('dir1' 456);
```

This example specifies multiple storage directories and directory sizes for oplog files:

``` pre
snappy> CREATE DISKSTORE STORE1 
    WRITEBUFFERSIZE 19292393
    QUEUESIZE 17374
    ('dir1' 456 , 'dir2', 'dir3' 532 );
```


