## Backup Guidelines and Prerequisites

-   SnappyData does not support backing up disk stores on a system with live transactions, or when concurrent DML statements are being executed.

-   Run a backup during a period of low activity in your system. The backup does not block any activities in the distributed system, but it does use file system resources on all hosts in your distributed system and can affect performance.

-   Optionally, compact your disk store before running the backup. See [Compacting Disk Store Log Files](../tables/persisting_table_data/compacting_disk_stores.md).

-   Only use the `snappy backup` command to create backup files from a running distributed system. Do not try to create backup files from a running system using file copy commands. You will get incomplete and unusable copies.

-   Back up to a directory that all members can access. Make sure the directory exists and has the proper permissions for your members to write to it and create subdirectories.

-   Make sure there is a `gemfirexd.properties` file for the distributed system in the directory where you will run the `snappy backup` command, or specify the multicast port or locator properties to connect to the cluster (for example, `-mcast-port=`*port_number*). The command will back up all disk stores in the specified distributed system.

-   In order to perform an incremental backup, you must use locators for SnappyData member discovery, rather than multicast.

-   The directory that you specify for backup can be used multiple times. Each backup first creates a top level directory for the backup, under the directory you specify, identified to the minute. This is useful especially when performing multiple, incremental backups of your database.

    You can specify a directory by one of two methods; the commands in the procedures below use the first method.

    -   Use a single physical location, such as a network file server, available to all members. For example:

        ``` 
        /export/fileServerDirectory/gemfireXDBackupLocation
        ```

    -   Use a directory that is local to all host machines in the system. Example:

        ``` 
        ./gemfireXDBackupLocation
        ```

		!!!Note
       		The same local directory must be available to each SnappyData member using the same path.
