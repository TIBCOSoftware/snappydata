# Optimizing a System with Disk Stores

Optimize availability and performance by following the guidelines in this section


-   SnappyData recommends the use of ext4 filesystems when operating on Linux platforms. The ext4 filesystem supports preallocation, which benefits disk startup performance. If you are using ext3 filesystems in latency-sensitive environments with high write throughput, you can improve disk startup performance by setting the MAXLOGSIZE property of a disk store to a value lower than the default 1 GB (see [CREATE DISKSTORE](../../../reference/sql_reference/create-diskstore.md)), or by using the system property `gemfire.preAllocateDisk=false` to disable preallocation (see [Preventing Disk Full Errors](../../../troubleshooting/prevent_disk_full_errors.md)).

-   When you start your system, start all the members that have persistent tables roughly at the same time. Create and use startup scripts for consistency and completeness.

-   Start locators and data stores with `-enable-network-partition-detection=true` to avoid persisting inconsistent data during network segmentation.

-   Shut down your system using the `snappy shut-down-all` command. This is an ordered shutdown that shuts down all data stores and accessors, but leaves locators running. When shutting down an entire system, a locator should be the last member to shut down (after all data stores have successfully stopped).

-   Decide on a file compaction policy and, if needed, develop procedures to monitor your files and execute regular compaction.

-   Decide on a backup strategy for your disk stores and follow it. You can back up by copying the files while the system is offline, or you can back up an online system using the `snappy backup` command.

-   If you drop or alter any persistent table while your disk store is offline, consider synchronizing the tables in your disk stores.


