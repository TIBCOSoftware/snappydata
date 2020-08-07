# compact-disk-store

Perform offline compaction of a single TIBCO ComputeDB disk store.

## Syntax

```pre
./bin/snappy compact-disk-store <diskStoreName> <directory>+ [-maxOplogSize=<int>]
```

## Description

!!! Note
	Do not perform offline compaction on the baseline directory of an incremental backup.</p>
When a CRUD operation is performed on a persistent/overflow table, the data is written to the log files. Any pre-existing operation record for the same row becomes obsolete, and TIBCO ComputeDB marks it as garbage. It compacts an old operation log by copying all non-garbage records into the current log and discarding the old files.

Manual compaction can be done for online and offline disk stores. For the online disk store, the current operation log is not available for compaction, no matter how much garbage it contains.

Offline compaction runs in the same way, but without the incoming CRUD operations. Also, because there is no current open log, the compaction creates a new one to get started.

!!! Note
	You must provide all of the directories in the disk store. If no oplog max size is specified, TIBCO ComputeDB uses the system default.</br> Offline compaction can consume a large amount of memory. If you get a java.lang.OutOfMemory error while running this command, you made need to increase the heap size by setting the `-Xmx` and `-Xms` options in the JAVA_ARGS environment variable. [Command Line Utilites](../../reference/command_line_utilities/store-launcher.md) provides more information about setting Java options.

</p>

## Example

```pre
./bin/snappy compact-disk-store myDiskStoreName  /firstDir  /secondDir   
maxOplogSize=maxMegabytesForOplog
```

The output of this command is similar to:

```pre
Offline compaction removed 12 records.
Total number of region entries in this disk store is: 7
```


