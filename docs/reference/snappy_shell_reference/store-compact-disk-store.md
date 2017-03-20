# compact-disk-store

Perform offline compaction of a single RowStore disk store.

##Syntax

``` pre
snappy-shell compact-disk-store <diskStoreName> <directory>+ [-maxOplogSize=<int>]
```

<a id="reference_13F8B5AFCD9049E380715D2EF0E33BDC__section_050663B03C0A4C42B07B4C5F69EAC95D"></a>
##Description

!!!Note:
	Do not perform offline compaction on the baseline directory of an incremental backup.</p>
When a CRUD operation is performed on a persistent/overflow table the data is written to the log files. Any pre-existing operation record for the same row becomes obsolete, and RowStore marks it as garbage. It compacts an old operation log by copying all non-garbage records into the current log and discarding the old files.

Manual compaction can be done for online and offline disk stores. For the online disk store, the current operation log is not available for compaction, no matter how much garbage it contains.

Offline compaction runs essentially in the same way, but without the incoming CRUD operations. Also, because there is no current open log, the compaction creates a new one to get started.

!!!Note:
	You must provide all of the directories in the disk store. If no oplog max size is specified, RowStore uses the system default.</br> Offline compaction can consume a large amount of memory. If you get a java.lang.OutOfMemory error while running this command, you made need to increase the heap size by setting the `-Xmx` and `-Xms` options in the JAVA\_ARGS environment variable. <a href="store-launcher.html#reference_9518856325F74F79B13674B8E060E6C5" class="xref" title="Use the snappy-shell command-line utility to launch RowStore utilities.">snappy-shell Launcher Commands</a> provides more information about setting Java options.

</p>

##Example

``` pre
snappy-shell compact-disk-store myDiskStoreName  /firstDir  /secondDir   
maxOplogSize=maxMegabytesForOplog
```

The output of this command is similar to:

``` pre
Offline compaction removed 12 records.
Total number of region entries in this disk store is: 7
```


