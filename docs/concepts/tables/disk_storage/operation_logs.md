# Disk Store Operation Logs


At creation, each operation log is initialized at the disk store's MAXLOGSIZE value, with the size divided between the `crf` and `drf` files. SnappyData only truncates the unused space on a clean shutdown (for example, `snappy rowstore server stop` or `snappy shut-down-all`).

<a id="operation_logs__section_C0B1391492394A908577C29772902A42"></a>
After the oplog is closed, SnappyData also attempts to created a `krf` file, which contains the key names as well as the offset for the value within the crf file. Although this file is not required for startup, if it is available, it will improve startup performance by allowing SnappyData to load the entry values in the background after the entry keys are loaded. See

When an operation log is full, SnappyData automatically closes it and creates a new log with the next sequence number. This is called *oplog rolling*. 

!!! Note:
   	Log compaction can change the names of the disk store files. File number sequencing is usually altered, with some existing logs removed or replaced by newer logs with higher numbering. SnappyData always starts a new log at a number higher than any existing number. </p>

The system rotates through all available disk directories to write its logs. The next log is always started in a directory that has not reached its configured capacity, if one exists.

<a id="operation_logs__section_8431984F4E6644D79292850CCA60E6E3"></a>

## When Disk Store oplogs Reach the Configured Disk Capacity

If no directory exists that is within its capacity limits, how SnappyData handles this depends on whether automatic compaction is enabled.

*   If AUTOCOMPACT is enabled (set to 'true), SnappyData creates a new oplog in one of the directories, going over the limit, and logs a warning that reports:

	    Even though the configured directory size limit has been exceeded a 
    	new oplog will be created. The current limit is of XXX. The current 
    	space used in the directory is YYY.

	!!! Note:
	    When auto-compaction is enabled, directory sizes do not limit how much disk space is used. SnappyData performs auto-compaction, which should free space, but the system may go over the configured disk limits. </p>

*   If auto-compaction is disabled, SnappyData does not create a new oplog. DML operations to tables block, and SnappyData logs the error:

		Disk is full and rolling is disabled. No space can be created

