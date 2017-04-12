# Disk Store State

Disk store access and management differs according to whether the store is online or offline.

When a member shuts down, its disk stores go offline. When the member starts up again, its disk stores come back online in the SnappyData cluster.

-   Online, a disk store is owned and managed by its member process.

-   Offline, the disk store is simply a collection of files in your host file system. The files are open to access by anyone with the right file system permissions. You can copy the files using your file system commands, for backup or to move your member’s disk store location. You can also run maintenance operations on the offline disk store, like file compaction and validation using the `snappy` utility. 

	!!! Note
    	The files for a disk store are used by SnappyData as a group. Treat them as a single entity. If you copy them, copy them all together. Do not change the file names or permissions. </br> When a disk store is offline, its data is unavailable to the SnappyData distributed system. For partitioned tables, the data is split between multiple members, so you can access the offline data only if you store replicas of the partitioned table on other members of the cluster.


