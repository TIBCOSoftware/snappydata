# Compacting Disk Store Log Files

You can configure automatic compaction for an operation log based on percentage of garbage content. You can also request compaction manually for online and offline disk stores.

## How Compaction Works

The following topics deal with compaction:

-   <a href="compacting_disk_stores.html#compacting_disk_stores__section_F7781CFEC3B342CFAB523ED130922233" class="xref">How Compaction Works</a>
-   <a href="compacting_disk_stores.html#compacting_disk_stores__section_98C6B6F48E4F4F0CB7749E426AF4D647" class="xref">Online Compaction Diagram</a>
-   <a href="compacting_disk_stores.html#compacting_disk_stores__section_96E774B5502648458E7742B37CA235FF" class="xref">Run Online Compaction</a>
-   <a href="compacting_disk_stores.html#compacting_disk_stores__section_25BDB098E9584EAA9BC6582597544726" class="xref">Run Offline Compaction</a>
-   <a href="compacting_disk_stores.html#compacting_disk_stores__section_D2374039480947C5AE4CC64167E60978" class="xref">Performance Benefits of Manual Compaction</a>
-   <a href="compacting_disk_stores.html#compacting_disk_stores__section_A9EE86F662EE4D46A327C336E901A0F2" class="xref">Directory Size Limits</a>
-   <a href="compacting_disk_stores.html#compacting_disk_stores__section_7A311038408440D49097B8FA4E2BCED9" class="xref">Example Compaction Run</a>

<a id="compacting_disk_stores__section_64BA304595364E38A28098EB09494531"></a>

When a DML operation is added to a disk store, any preexisting operation record for the same record becomes obsolete, and SnappyData marks it as garbage. For example, when you update a record, the update operation is added to the store. If you delete the record later, the delete operation is added and the update operation becomes garbage. SnappyData does not remove garbage records as it goes, but it tracks the percentage of garbage in each operation log, and provides mechanisms for removing garbage to compact your log files.

SnappyData compacts an old operation log by copying all non-garbage records into the current log and discarding the old files. As with logging, oplogs are rolled as needed during compaction to stay within the MAXLOGSIZE setting.

You can configure the system to automatically compact any closed operation log when its garbage content reaches a certain percentage. You can also manually request compaction for online and offline disk stores. For the online disk store, the current operation log is not available for compaction, no matter how much garbage it contains.

<a id="compacting_disk_stores__section_98C6B6F48E4F4F0CB7749E426AF4D647"></a>

## Online Compaction Diagram

<img src="../common/images/diskStores-3.gif" id="compacting_disk_stores__image_7E34CC58B13548B196DAA15F5B0A0ECA" class="image" />

Offline compaction runs essentially in the same way, but without the incoming DML operations. Also, because there is no current open log, the compaction creates a new one to get started.

<a id="compacting_disk_stores__section_96E774B5502648458E7742B37CA235FF"></a>

Run Online Compaction
---------------------

Old log files become eligible for online compaction when their garbage content surpasses a configured percentage of the total file. A record is garbage when its operation is superseded by a more recent operation for the same record. During compaction, the non-garbage records are added to the current log along with new DML operations. Online compaction does not block current system operations.

-   **Run automatic compaction**. When `AUTOCOMPACT` is true, SnappyData automatically compacts each oplog when its garbage content surpasses the `COMPACTIONTHRESHOLD`. Automatic compaction takes cycles from your other operations, so you may want to disable it and only do manual compaction, to control the timing.
-   **Run manual compaction**. To run manual compaction:
    -   Set the disk store attribute `ALLOWFORCECOMPACTION` to true. This causes SnappyData to maintain extra data about the files so that it can compact on demand. This is disabled by default to save space. You can run manual online compaction at any time while the system is running. Oplogs eligible for compaction based on the `COMPACTIONTHRESHOLD` are compacted into the current oplog.

    -   Run manual compaction as needed. You can compact all online disk stores in a distributed system from the command-line. For example:

        ``` pre
        snappy compact-all-disk-stores
        ```

        !!!Note:
        	This `snappy` command requires a local `gemfirexd.properties` file that contains properties to locate the distributed system. Or, specify the multicast port or locator properties to connect to the cluster (for example, `-mcast-port=`*port\_number*). </p>

<a id="compacting_disk_stores__section_25BDB098E9584EAA9BC6582597544726"></a>

## Run Offline Compaction

!!! Note
	Do not perform offline compaction on the baseline directory of an incremental backup.</p>

Offline compaction is a manual process. All log files are compacted as much as possible, regardless of how much garbage they hold. Offline compaction creates new log files for the compacted log records.

Use this syntax to compact individual offline disk stores:

``` pre
snappy compact-disk-store myDiskStoreName /firstDir /secondDir -maxOplogSize=maxMegabytesForOplog
```

You must provide all of the directories in the disk store. If no oplog max size is specified, SnappyData uses the system default.

Offline compaction can take a lot of memory. If you get a `java.lang.OutOfMemory` error while running this, you made need to increase your heap size. See the `snappy` command help for instructions on how to do this.

## Performance Benefits of Manual Compaction

You can improve performance during busy times if you disable automatic compaction and run your own manual compaction during lighter system load or during downtimes. You could run the API call after your application performs a large set of data operations. You could run `snappy compact-all-disk-stores` every night when system use is very low.

To follow a strategy like this, you need to set aside enough disk space to accommodate all non-compacted disk data. You might need to increase system monitoring to make sure you do not overrun your disk space. You may be able to run only offline compaction. If so, you can set `ALLOWFORCECOMPACTION` to false and avoid storing the information required for manual online compaction.

## Directory Size Limits

If you reach the disk directory size limits during compaction:

-   For automatic compaction, the system logs a warning, but does not stop.
-   For manual compaction, the operation stops and returns a `DiskAccessException` to the calling process, reporting that the system has run out of disk space.

<a id="compacting_disk_stores__section_7A311038408440D49097B8FA4E2BCED9"></a>

## Example Compaction Run

In this example offline compaction run listing, the disk store compaction had nothing to do in the `*_3.*` files, so they were left alone. The `*_4.*` files had garbage records, so the oplog from them was compacted into the new `*_5.*` files.

``` pre
bash-2.05$ ls -ltra backupDirectory
total 28
-rw-rw-r--   1 jpearson users          3 Apr  7 14:56 BACKUPds1_3.drf
-rw-rw-r--   1 jpearson users         25 Apr  7 14:56 BACKUPds1_3.crf
drwxrwxr-x   3 jpearson users       1024 Apr  7 15:02 ..
-rw-rw-r--   1 jpearson users       7085 Apr  7 15:06 BACKUPds1.if
-rw-rw-r--   1 jpearson users         18 Apr  7 15:07 BACKUPds1_4.drf
-rw-rw-r--   1 jpearson users       1070 Apr  7 15:07 BACKUPds1_4.crf
drwxrwxr-x   2 jpearson users        512 Apr  7 15:07 .

bash-2.05$ snappy validate-disk-store ds1 backupDirectory
/root: entryCount=6
/partitioned_region entryCount=1 bucketCount=10
Disk store contains 12 compactable records.
Total number of region entries in this disk store is: 7

bash-2.05$ snappy compact-disk-store ds1 backupDirectory
Offline compaction removed 12 records.
Total number of region entries in this disk store is: 7

bash-2.05$ ls -ltra backupDirectory
total 16
-rw-rw-r--   1 jpearson users          3 Apr  7 14:56 BACKUPds1_3.drf
-rw-rw-r--   1 jpearson users         25 Apr  7 14:56 BACKUPds1_3.crf
drwxrwxr-x   3 jpearson users       1024 Apr  7 15:02 ..
-rw-rw-r--   1 jpearson users          0 Apr  7 15:08 BACKUPds1_5.drf
-rw-rw-r--   1 jpearson users        638 Apr  7 15:08 BACKUPds1_5.crf
-rw-rw-r--   1 jpearson users       2788 Apr  7 15:08 BACKUPds1.if
drwxrwxr-x   2 jpearson users        512 Apr  7 15:09 .
bash-2.05$
```
