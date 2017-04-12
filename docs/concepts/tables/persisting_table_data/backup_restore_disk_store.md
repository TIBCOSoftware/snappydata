# Backing Up and Restoring Disk Stores

When you invoke the `snappy backup` command, SnappyData backs up disk stores for all members that are running in the distributed system at that time. Each member with persistent data creates a backup of its own configuration and disk stores.

-   <a href="#backup_restore_disk_store__section_63AB5917BF24432898A79DBE8E4071FF" class="xref">Backup Guidelines and Prerequisites</a>

-   <a href="#backup_restore_disk_store__section_DFAA329BA0C54FA2ABD664BB1ED04C36" class="xref">Performing a Full Online Backup</a>

-   <a href="#backup_restore_disk_store__secincback" class="xref">Performing an Incremental Backup</a>

-   <a href="#backup_restore_disk_store__section_C08E52E65DAD4CD5AE076BBDCF1DB340" class="xref">What a Full Online Backup Saves</a>

-   <a href="#backup_restore_disk_store__section_59E23EEA4AB24374A45B99A8B44FD49B" class="xref">What an Incremental Online Backup Saves</a>

-   <a href="#backup_restore_disk_store__section_6F998080AF7640D1A9E951D155A75E3A" class="xref">Offline Members: Manual Catch-Up to an Online Backup</a>

-   <a href="#backup_restore_disk_store__section_D08DC489B9D947DE97B8F96261E4A977" class="xref">Restore an Online Backup</a>

-   <a href="#backup_restore_disk_store__section_A7981F25371A40C5A7A298768E9636DF" class="xref">Offline File Backup and Restore</a>

<a id="backup_restore_disk_store__section_63AB5917BF24432898A79DBE8E4071FF"></a>

## Backup Guidelines and Prerequisites

-   SnappyData does not support backing up disk stores on a system with live transactions, or when concurrent DML statements are being executed.

-   Run a backup during a period of low activity in your system. The backup does not block any activities in the distributed system, but it does use file system resources on all hosts in your distributed system and can affect performance.

-   Optionally, compact your disk store before running the backup. See [Compacting Disk Store Log Files](compacting_disk_stores.md).

-   Only use the `snappy backup` command to create backup files from a running distributed system. Do not try to create backup files from a running system using file copy commands. You will get incomplete and unusable copies.

-   Back up to a directory that all members can access. Make sure the directory exists and has the proper permissions for your members to write to it and create subdirectories.

-   Make sure there is a `gemfirexd.properties` file for the distributed system in the directory where you will run the `snappy backup` command, or specify the multicast port or locator properties to connect to the cluster (for example, `-mcast-port=`*port\_number*). The command will back up all disk stores in the specified distributed system.

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

<a id="backup_restore_disk_store__section_DFAA329BA0C54FA2ABD664BB1ED04C36"></a>

## Performing a Full Online Backup

1.  Stop all transactions running in your distributed system, and do not execute DML statements during the backup. SnappyData does not support backing up a disk store while live transactions are taking place or when concurrent DML statements are being executed.

2.  Run the `snappy backup` command, providing your backup directory location. For example:

    ``` 
    $ snappy backup /export/fileServerDirectory/gemfireXDBackupLocation
         -locators=machine.name[26340]
    ```

3.  Read the message that reports on the success of the operation.

    If the operation is successful, you see a message like this:

    ``` 
    Connecting to distributed system: locators=machine.name[26340]
    The following disk stores were backed up:
        DiskStore at hosta.machine.name /home/user/dir1
        DiskStore at hostb.machine.name /home/user/dir2
    Backup successful.
    ```

    If the operation does not succeed at backing up all known members, you see a message like this:

    ``` 
    Connecting to distributed system: locators=machine.name[26357]
    The following disk stores were backed up:
        DiskStore at hosta.machine.name /home/user/dir1
        DiskStore at hostb.machine.name /home/user/dir2
    The backup may be incomplete. The following disk stores are not online:
        DiskStore at hostc.machine.name /home/user/dir3
    ```

    A member that fails to complete its backup is noted in this ending status message and leaves the file `INCOMPLETE_BACKUP` in its highest level backup directory. Offline members leave nothing, so you only have this message from the backup operation itself. Although offline members cannot back up their disk stores, a complete backup can be obtained if at least one copy of the data is available in a running member.

<a id="backup_restore_disk_store__secincback"></a>

## Performing an Incremental Backup

1.  Stop all transactions running in your distributed system, and do not execute DML statements during the backup. SnappyData does not support backing up a disk store while live transactions are taking place or when concurrent DML statements are being executed.
2.  Perform a full online backup to create a a baseline backup for the distributed system:

    ``` 
    $ snappy backup /export/fileServerDirectory/gemfireXDBackupLocation
         -locators=machine.name[26340]
    ```

3.  To perform an incremental backup, execute the `snappy backup` command but specify the baseline directory as well as your incremental backup directory (they can be the same directory). For example:

    ``` 
    $ snappy backup -baseline=/export/fileServerDirectory/gemfireXDBackupLocation /export/fileServerDirectory/gemfireXDBackupLocation 
      -locators=machine.name[26340] 
    ```

    The tool reports on the success of the operation. If the operation is successful, you see a message like this:

    ``` 
    The following disk stores were backed up:
        DiskStore at hosta.gemstone.com /home/user/dir1
        DiskStore at hostb.gemstone.com /home/user/dir2
    Backup successful.
    ```

    If the operation does not succeed at performing an incremental backup on all known members, you see a message like this:

    ``` 
    The following disk stores were backed up:
        DiskStore at hosta.gemstone.com /home/user/dir1
        DiskStore at hostb.gemstone.com /home/user/dir2
    The backup may be incomplete. The following disk stores are not online:
        DiskStore at hostc.gemstone.com /home/user/dir3
    ```

    A member that fails to complete its backup is noted in this ending status message and leaves the file `INCOMPLETE_BACKUP`. The next time you perform a backup operation a full backup will be performed.

4.  Make additional incremental backups as necessary by executing the same `snappy backup` command described in the previous step.

<a id="backup_restore_disk_store__section_C08E52E65DAD4CD5AE076BBDCF1DB340"></a>

## What a Full Online Backup Saves

For each member with persistent data, the backup includes:

-   Disk store files for all stores containing persistent tables.

-   Configuration files from the member startup (<span class="ph filepath">gemfirexd.properties</span>). These configuration files are not automatically restored, to avoid interfering with any more recent configurations. In particular, if these are extracted from a master `jar` file, copying the separate files into your working area could override the files in the `jar`.

-   A restore script, written for the member’s operating system, that copies the files back to their original locations. For example, in Windows, the file is `restore.bat` and in Linux, it is `restore.sh`.

<a id="backup_restore_disk_store__section_59E23EEA4AB24374A45B99A8B44FD49B"></a>

## What an Incremental Online Backup Saves

An incremental backup saves the difference between the last backup and the current data. An incremental backup copies only operation logs that are not already present in the baseline directories for each member. For incremental backups, the restore script contains explicit references to operation logs in one or more previously-chained incremental backups. When the restore script is run from an incremental backup, it also restores the operation logs from previous incremental backups that are part of the backup chain.

If members are missing from the baseline directory because they were offline or did not exist at the time of the baseline backup, those members place full backups of all their files into the incremental backup directory.

!!! Note:
	SnappyData does not support taking incremental backups on systems with live transactions, or when concurrent DML statements are being executed.</p>

<a id="backup_restore_disk_store__section_6F998080AF7640D1A9E951D155A75E3A"></a>

## Offline Members: Manual Catch-Up to an Online Backup

If you must have a member offline during an online backup, you can manually back up its disk stores. Do one of the following:

-   Keep the member’s backup and restore separated, doing offline manual backup and offline manual restore, if needed.

-   Bring this member’s files into the online backup framework manually and create a restore script by hand, from a copy of another member’s script:

    1.  Duplicate the directory structure of a backed up member for this member.

    2.  Rename directories as needed to reflect this member’s particular backup, including disk store names.

    3.  Clear out all files but the restore script.

    4.  Copy in this member’s files.

    5.  Modify the restore script to work for this member.

<a id="backup_restore_disk_store__section_D08DC489B9D947DE97B8F96261E4A977"></a>

## Restore an Online Backup

The restore script included in the online backup copies files back to their original locations. You can do this manually if you wish.

**Prerequisites**

-   Your members are offline and the system is down.

-   Restore all SnappyData members that were active in the original system. (For example, you cannot use disk store backups to create a new system that hosts fewer datastores than were in the original distributed system.)

**Procedure**

1.  Navigate to the backup subdirectory with the timestamp of the backup that you want to restore. For example, if you performed multiple incremental backups, navigate to the latest backup directory in order to restore your system to the last available backup.

2.  Read the restore scripts to see where they will place the files and make sure the destination locations are ready. The restore scripts refuse to copy over files with the same names.

3.  Run each restore script on the host where the backup originated.

    In Windows, the file is `restore.bat` and in Linux, it is `restore.sh`.

    The restore copies the files back to their original location.

4.  Repeat this procedure as necessary for other members of the distributed system.

5.  After all disk stores have been restored, restart all members of the original cluster.

<a id="backup_restore_disk_store__section_A7981F25371A40C5A7A298768E9636DF"></a>

## Offline File Backup and Restore

To back up your offline system:

 1.  Validate, and consider compacting your disk stores before backing them up. See [Compacting Disk Store Log Files](compacting_disk_stores.md).

 2.  Copy all disk store files, and any other files you want to save, to your backup locations.

To restore a backup of an offline system:

 1.  Make sure the system is either down or not using the directories you will use for the restored files.

 2.  Make sure your members are configured to use the directories where you put the files.

 3.  Reverse your backup file copy procedure, copying all the backed up files into the directories you want to use.

 4.  Start the system members.

