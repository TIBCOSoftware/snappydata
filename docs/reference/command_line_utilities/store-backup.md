# backup
Creates a backup of operational disk stores for all members running in the distributed system. Each member with persistent data creates a backup of its own configuration and disk stores.

<a id="reference_13F8B5AFCD9049E380715D2EF0E33BDC__section_06BC55B8DB414173BBD71BEFB9F8F1BD"><p !!!
!!!Note:
	 SnappyData does not support backing up disk stores on systems with live transactions, or when concurrent DML statements are being executed. See [Backing Up and Restoring Disk Stores](../../concepts/backup/backup_restore_disk_store.md).</p>
-   [Syntax](store-backup.md#syntax)
-   [Description](store-backup.md#description)
-   [Prerequisites and Best Practices](store-backup.md#prereq)
-   [Specifying the Backup Directory](store-backup.md#backup_directory)
-   [Example](store-backup.md#example)
-   [Output Messages from snappy backup](store-backup.md#output_messages)
-   [Backup Directory Structure and Its Contents](store-backup.md#directory_structure)
-   [Restoring an Online Backup](store-backup.md#restore_online_backup)

<a id="syntax"></a>

#Syntax

Use the mcast-port and -mcast-address, or the -locators options, on the command line to connect to the SnappyData cluster.

``` pre
snappy backup [-baseline=<baseline directory>] <target directory> [-J-D<vmprop>=<prop-value>]
 [-mcast-port=<port>] [-mcast-address=<address>]
 [-locators=<addresses>] [-bind-address=<address>] [-<prop-name>=<prop-value>]*
```

Alternatively, you can specify these and other distributed system properties in a <span class="ph filepath">gemfirexd.properties</span> file that is available in the directory where you run the `snappy` command.

The table describes options for snappy backup.

|Option|Description|
|-|-|
|-baseline|The directory that contains a baseline backup used for comparison during an incremental backup. The baseline directory corresponds to the date when the original backup command was performed, rather than the backup location you specified (for example, a valid baseline directory might resemble <span class="ph filepath">/export/fileServerDirectory/gemfireXDBackupLocation/2012-10-01-12-30</span>).</br>An incremental backup operation backs up any data that is not already present in the specified `-baseline` directory. If the member cannot find previously backed up data or if the previously backed up data is corrupt, then command performs a full backup on that member. (The command also performs a full backup if you omit the `-baseline` option.|
|&lt;target-directory&gt;|The directory in which SnappyData stores the backup content. See [Specifying the Backup Directory](store-backup.md#backup_directory).|
|-mcast-port|Multicast port used to communicate with other members of the distributed system. If zero, multicast is not used for member discovery (specify `-locators` instead). </br>Valid values are in the range 0–65535, with a default value of 10334.|
|-mcast-address|Multicast address used to discover other members of the distributed system. This value is used only if the `-locators` option is not specified.</br>The default multicast address is 239.192.81.1.|
|-locators|List of locators used to discover members of the distributed system. Supply all locators as comma-separated host:port values.|
|-bind-address|The address to which this peer binds for receiving peer-to-peer messages. By default SnappyData uses the hostname, or localhost if the hostname points to a local loopback address.|
|-prop-name|Any other SnappyData distributed system property.|

<a id="description"></a>

#Description

An online backup saves the following:

-   For each member with persistent data, the backup includes disk store files for all stores containing persistent table data.
-   Configuration files from the member startup.
-   gemfirexd.properties, with the properties the member was started with.
-   A restore script, written for the member's operating system, that copies the files back to their original locations. For example, in Windows, the file is restore.bat and in Linux, it is restore.sh.

<a id="prereq"></a>

#Prerequisites and Best Practices

-   Run this command during a period of low activity in your system. The backup does not block system activities, but it uses file system resources on all hosts in your distributed system and can affect performance.
-   Do not try to create backup files from a running system using file copy commands. You will get incomplete and unusable copies.
-   Make sure the target backup directory directory exists and has the proper permissions for your members to write to it and create subdirectories.
-   You might want to compact your disk store before running the backup. See the [compact-all-disk-stores](store-compact-all-disk-stores.md#reference_13F8B5AFCD9049E380715D2EF0E33BDC) command.
-   Make sure that those SnappyData members that host persistent data are running in the distributed system. Offline members cannot back up their disk stores. (A complete backup can still be performed if all table data is available in the running members.)

<a id="backup_directory"></a>

#Specifying the Backup Directory

The directory you specify for backup can be used multiple times. Each backup first creates a top level directory for the backup, under the directory you specify, identified to the minute. You can use one of two formats:

-   Use a single physical location, such as a network file server. (For example, /export/fileServerDirectory/snappyStoreBackupLocation).
-   Use a directory that is local to all host machines in the system. (For example, ./snappyStoreBackupLocation).

<a id="example"></a>

#Example

Using a backup directory that is local to all host machines in the system:

``` pre
snappy backup  ./snappyStoreBackupLocation
  -locators=machine[26340]
```

See also [Backing Up and Restoring Disk Stores](../../concepts/backup/backup_restore_disk_store.md#backup_restore_disk_store).

To perform an incremental backup at a later time:

``` pre
snappy backup -baseline=./snappyStoreBackupLocation/2012-10-01-12-30 ./snappyStoreBackupLocation 
  -locators=machine[26340] 
```

!!! Note:
	SnappyData does not support taking incremental backups on systems with live transactions, or when concurrent DML statements are being executed.</p>

<a id="output_messages"></a>

#Output Messages from snappy backup

When you run `snappy backup`, it reports on the outcome of the operation.

If any members were offline when you run `snappy backup`, you get this message:

``` pre
The backup may be incomplete. The following disk
stores are not online:  DiskStore at hostc.pivotal.com
/home/user/dir3
```

A complete backup can still be performed if all table data is available in the running members.

The tool reports on the success of the operation. If the operation is successful, you see a message like this:

``` pre
Connecting to distributed system: locators=machine26340
The following disk stores were backed up:
DiskStore at hosta.pivotal.com /home/user/dir1
DiskStore at hostb.pivotal.com /home/user/dir2
Backup successful.
```

If the operation does not succeed at backing up all known members, you see a message like this:

``` pre
Connecting to distributed system: locators=machine26357
The following disk stores were backed up:
DiskStore at hosta.pivotal.com /home/user/dir1
DiskStore at hostb.pivotal.com /home/user/dir2
The backup may be incomplete. The following disk stores are not online:
DiskStore at hostc.pivotal.com /home/user/dir3
```

A member that fails to complete its backup is noted in this ending status message and leaves the file INCOMPLETE_BACKUP in its highest level backup directory.

<a id="directory_structure"></a>

#Backup Directory Structure and Its Contents

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

#Restoring an Online Backup

The restore script (<span class="ph filepath">restore.sh</span> or <span class="ph filepath">restore.bat</span>) copies files back to their original locations. You can do this manually if you wish:

1.  Restore your disk stores when your members are offline and the system is down.
2.  Read the restore scripts to see where they will place the files and make sure the destination locations are ready. The restore scripts refuse to copy over files with the same names.
3.  Run the restore scripts. Run each script on the host where the backup originated.

The restore copies these back to their original location.
