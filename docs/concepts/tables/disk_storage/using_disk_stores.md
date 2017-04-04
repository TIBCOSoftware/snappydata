# Guidelines for Designing Disk Stores

Work with your system designers and developers to plan for disk storage requirements in testing and production database systems. Work with host system administrators to determine where to place your disk store directories on each peer computer.


<a id="defining_disk_stores__section_75E09F6C4D08482484B7D31426D48DE6"></a>
Consider these guidelines when designing disk stores:

- <mark>  As a best practice, set enable-network-partition-detection</a> to true in any SnappyData distributed system that persists data. When partition detection is disabled, network segmentation can cause subgroups of members to work on inconsistent data. Restarting those members then fails with a `ConflictingPersistentDataException`, because members in the subgroups persisted inconsistent data to local disk store files. See [Detecting and Handling Network Segmentation (&quot;Split Brain&quot;) ]TO BE CONFIRMED and [Member Startup Problems]</mark>

-   Tables can be overflowed, persisted, or both. For efficiency, place table data that is overflowed on one disk store with a dedicated physical disk. Place table data that is persisted, or persisted and overflowed, on another disk store with on a different physical disk.

    For example, gateway sender, AsyncEventListener, and DBSynchronizer queues are always overflowed and may be persisted. Assign them to overflow disk stores if you do not persist, and to persistence disk stores if you do. Ensure that each disk store resides on a separate physical disk, for best performance.

-   When calculating your disk requirements, consider your table modification patterns and compaction strategy. SnappyData creates each oplog file at the specified MAXLOGSIZE. Obsolete DML operations are only removed from the oplogs during compaction, so you need enough space to store all operations that are done between compactions. For tables where you are doing a mix of updates and deletes, if you use automatic compaction, a good upper bound for the required disk space is

	    (1 / (1 - (compaction_threshold/100)) ) * data size

    where data size is the total size of all the table data you store in the disk store. So, for the default COMPACTIONTHRESHOLD of 50, the disk space is roughly twice your data size. The compaction thread could lag behind other operations, causing disk use to rise above the threshold temporarily. If you disable automatic compaction, the amount of disk required depends on how many obsolete operations accumulate between manual compactions.

-   Based on your anticipated disk storage requirements and the available disks on your host systems:

    -   Make sure the new storage does not interfere with other processes that use disk on your systems. If possible, store your files to disks that are not used by other processes, including virtual memory or swap space. If you have multiple disks available, for the best performance, place one directory on each disk.

    -   Use different directories for different peers that run on the same computer. You can use any number of directories for a single disk store.

-   Choose disk store names that reflect how the stores should be used and that work for your operating systems. Disk store names are used in the disk file names:

    -   Use disk store names that satisfy the file naming requirements for your operating system. For example, if you store your data to disk in a Windows system, your disk store names could not contain any of these reserved characters, &lt; &gt; : " / \\ | ? \*.

    -   Do not use very long disk store names. The full file names must fit within your operating system limits. On Linux, for example, the standard limitation is 255 characters.

-   Create each disk store with [CREATE DISKSTORE](../../../reference/sql_reference/create-diskstore.md) before you create persistent or overflow tables.

-   You may choose to parallelize disk access for oplog and overflow files using by targeting disk store files to multiple logical disk partitions.

-   SnappyData disk store files must be highly available. Back up disk store files on a regular schedule, either by copying the files while the system is offline, or by using the [backup](../../../reference/snappy_shell_reference/store-backup.md) command to perform online backups.

SnappyData peers in the cluster manage their local disk stores using the properties you specify in the [CREATE DISKSTORE](../../../reference/sql_reference/create-diskstore.md) statement.

After you create named disk stores, you can create tables that persist or overflow their data to disk stores.

See More:

* [CREATE DISKSTORE](../../../reference/sql_reference/create-diskstore.md)

* [CREATE TABLE](../../../reference/sql_reference/create-table.md)

* [Persisting Table Data to a Disk Store](../../../concepts/tables/disk_storage/persist_table.md)

* [Evicting Table Data from Memory](../../../concepts/tables/evicting_table_data_from_memory.md)
