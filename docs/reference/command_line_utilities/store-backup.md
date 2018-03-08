# backup and restore
Creates a backup of operational disk stores for all members running in the distributed system. Each member with persistent data creates a backup of its own configuration and disk stores.

!!!Note:
	SnappyData does not support backing up disk stores on systems with live transactions, or when concurrent DML statements are being executed.</br> 
       If a backup of live transaction or concurrent DML operations, is performed, there is a possibility of partial commits or partial changes of DML operations appearing in the backups.

-   [Syntax](store-backup.md#syntax)

-   [Description](store-backup.md#description)

-   [Prerequisites and Best Practices](store-backup.md#prereq)

-   [Specifying the Backup Directory](store-backup.md#backup_directory)

-   [Example](store-backup.md#example)

-   [Output Messages from snappy backup](store-backup.md#output_messages)

-   [Backup Directory Structure and Its Contents](store-backup.md#directory_structure)

-   [Restoring an Online Backup](store-backup.md#restore_online_backup)

<a id="syntax"></a>

## Syntax

Use the `-locator` option, on the command line to connect to the SnappyData cluster.

``` pre
snappy backup [-baseline=<baseline directory>] <target directory> [-J-D<vmprop>=<prop-value>]
 <-locators=<addresses>> [-bind-address=<address>] [-<prop-name>=<prop-value>]*
```

!!! Note
	
	* The <_target directory_> must be provided immediately after `- snappy backup [-baseline]` followed by the other arguments. `-baseline` is optional.
	* -J is a generic prefix for all JVM properties.

The table describes options for `snappy backup`:

|Option|Description|
|-|-|
|baseline|The directory that contains a baseline backup used for comparison during an incremental backup. The baseline directory corresponds to the date when the original backup command was performed, rather than the backup location you specified (for example, a valid baseline directory might resemble /export/fileServerDirectory/SnappyDataBackupLocation/2012-10-01-12-30).</br>An incremental backup operation backs up any data that is not already present in the specified `-baseline` directory. If the member cannot find previously backed up data or if the previously backed up data is corrupt, then command performs a full backup on that member. The command also performs a full backup if you omit the `-baseline` option.|
|target-directory|The directory in which SnappyData stores the backup content. See [Specifying the Backup Directory](store-backup.md#backup_directory).|
|locators| List of locators used to discover members of the distributed system. Supply all locators as comma-separated host:port values. The port is the `peer-discovery-port` used when starting the cluster (default 10334). This is a mandatory field.|
|bind-address|The address to which this peer binds for receiving peer-to-peer messages. By default SnappyData uses the hostname, or localhost if the hostname points to a local loopback address.|
|prop-name|Any other SnappyData distributed system property. </br>For example, `-locators=localhost:10334`|
|-J-D<name>=<value>|Sets Java system property. For example: `-J-Dgemfire.ack-wait-threshold=20`|
|-J|Prefix for any JVM property. For example `-J-Xmx4g`|

<a id="description"></a>

## Description

An online backup saves the following:

-   For each member with persistent data, the backup includes disk store files for all stores containing persistent table data.

-   Configuration files from the member startup.

-   A restore script, written for the member's operating system, that copies the files back to their original locations. For example, in Windows, the file is restore.bat and in Linux, it is restore.sh.

<a id="prereq"></a>

## Prerequisites and Best Practices

-   Run this command during a period of low activity in your system. The backup does not block system activities, but it uses file system resources on all hosts in your distributed system and can affect performance.

-   Do not try to create backup files from a running system using file copy commands. You will get incomplete and unusable copies.

-   Make sure the target backup directory exists and has the proper permissions for your members to write to it and create subdirectories. 

-   You might want to compact your disk store before running the backup.<!-- See the [compact-all-disk-stores](store-compact-all-disk-stores.md#reference_13F8B5AFCD9049E380715D2EF0E33BDC) command.-->

-   Make sure that those SnappyData members that host persistent data are running in the distributed system. Offline members cannot back up their disk stores. (A complete backup can still be performed if all table data is available in the running members.)

<a id="backup_directory"></a>

## Specifying the Backup Directory

The directory you specify for backup can be used multiple times. Each backup first creates a top level directory for the backup, under the directory you specify, identified to the minute. You can use one of two formats:

-   Use a single physical location, such as a network file server. (For example, /export/fileServerDirectory/snappyStoreBackupLocation).

-   Use a directory that is local to all host machines in the system. (For example, ./snappyStoreBackupLocation).

<a id="example"></a>

## Example

Using a backup directory that is local to all host machines in the system:

``` pre
snappy backup  ./snappyStoreBackupLocation
  -locators=localhost:10334
```
<!--
See also [Backing Up and Restoring Disk Stores](../../concepts/backup/backup_restore_disk_store.md#backup_restore_disk_store).-->

To perform an incremental backup at a later time:

``` pre
snappy backup -baseline=./snappyStoreBackupLocation/2012-10-01-12-30 ./snappyStoreBackupLocation 
  -locators=localhost:10334
```

!!! Note:
	SnappyData does not support taking incremental backups on systems with live transactions, or when concurrent DML statements are being executed.</p>

<a id="output_messages"></a>

## Output Messages from snappy backup

When you run `snappy backup`, it reports on the outcome of the operation.

If any members were offline when you run `snappy backup`, you get this message:

``` pre
The backup may be incomplete. The following disk
stores are not online: 93050768-514d-4b20-99e5-c8c9c0156ae9 [localhost:/home/user1/snappydata/build-artifacts/scala-2.11/snappy/work1/localhost-locator-1/.]  
```
A complete backup can still be performed if all table data is available in the running members.

The tool reports on the success of the operation. If the operation is successful, you see a message like this:

``` pre
Connecting to distributed system: -locators=localhost:10334
The following disk stores were backed up:
93050768-514d-4b20-99e5-c8c9c0156ae9 [localhost:/home/user1/snappydata/build-artifacts/scala-2.11/snappy/work1/localhost-locator-1/.]
	4860bb01-4b0a-4025-80c3-17708770d933 [localhost:/home/user1/snappydata/build-artifacts/scala-2.11/snappy/work1/localhost-locator-1/./datadictionary]
	9c656fdd-aa7b-4f08-926b-768424ec672a [snappy-user:/home/user1/snappydata/build-artifacts/scala-2.11/snappy/work1/localhost-server-1/./datadictionary]
	45ddc031-ae2a-4e8a-9b28-dad4fa1fd4cf [snappy-user:/home/user1/snappydata/build-artifacts/scala-2.11/snappy/work1/localhost-server-1/.]
Backup successful.
```

If the operation does not succeed at backing up all known members, you see a message like this:

``` pre
Connecting to distributed system: -locators=localhost:10334
The following disk stores were backed up:
93050768-514d-4b20-99e5-c8c9c0156ae9 [localhost:/home/user1/snappydata/build-artifacts/scala-2.11/snappy/work1/localhost-locator-1/.]
	4860bb01-4b0a-4025-80c3-17708770d933 [localhost:/home/user1/snappydata/build-artifacts/scala-2.11/snappy/work1/localhost-locator-1/./datadictionary]
	9c656fdd-aa7b-4f08-926b-768424ec672a [snappy-user:/home/user1/snappydata/build-artifacts/scala-2.11/snappy/work1/localhost-server-1/./datadictionary]
	45ddc031-ae2a-4e8a-9b28-dad4fa1fd4cf [snappy-user:/home/user1/snappydata/build-artifacts/scala-2.11/snappy/work1/localhost-server-1/.]
The backup may be incomplete. The following disk stores are not online:
```

A member that fails to complete its backup is noted in this ending status message and leaves the file INCOMPLETE_BACKUP in its highest level backup directory.

<a id="directory_structure"></a>

## Backup Directory Structure and Its Contents

Below is the structure of files and directories backed up in a distributed system:

``` pre
 2011-05-02-18-10 /:
pc15_8933_v10_10761_54522
2011-05-02-18-10/pc15_8933_v10_10761_54522:
config diskstores README.txt restore.sh
2011-05-02-18-10/pc15_8933_v10_10761_54522/config:
gemfirexd.properties
2011-05-02-18-10/pc15_8933_v10_10761_54522/diskstores:
GFXD_DD_DISKSTORE
2011-05-02-18-10/pc15_8933_v10_10761_54522/diskstores/GFXD_DD_DISKSTORE:
dir0
2011-05-02-18-10/pc15_8933_v10_10761_54522/diskstores/GFXD_DD_DISKSTORE/dir0:
BACKUPGFXD-DD-DISKSTORE_1.crf
BACKUPGFXD-DD-DISKSTORE_1.drf BACKUPGFXD-DD-DISKSTORE_2.crf
BACKUPGFXD-DD-DISKSTORE_2.drf BACKUPGFXD-DD-DISKSTORE.if
```

<a id="restore_online_backup"></a>

## Restoring Files

The restore script (restore.sh restore.bat) copies files back to their original locations. You can do this manually if you wish:

1.  Restore your disk stores when your members are offline and the system is down.

2.  Read the restore scripts to see where the files are placed and make sure the destination locations are ready. The restore scripts does not copy over files with the same names.

3.  Run the restore scripts. Run each script on the host where the backup originated.

The restore operation copies the files back to their original location.
