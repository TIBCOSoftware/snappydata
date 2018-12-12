# Backup and Restore

Creates a backup of operational disk stores for all members running in the distributed system. Each member with persistent data creates a backup of its own configuration and disk stores.

An online backup saves the following:

- For each member with persistent data, the backup includes disk store files for all stores containing persistent table data.
- Configuration files from the member startup.
- A restore script (restore.sh) copies the files back to their original locations.

!!! Note
	- SnappyData does not support backing up disk stores on systems with live transactions, or when concurrent DML statements are being executed. </br>If a backup of the live transaction or concurrent DML operations, is performed, there is a possibility of partial commits or partial changes of DML operations appearing in the backups.
	- SnappyData does not support taking incremental backups on systems with live transactions, or when concurrent DML statements are being executed.

- [Guidelines](#guidelines)

 -   [Specifying the Backup Directory](#backup-directory)

 -   [Backup Directory Structure and Contents](#directory-structure)

- [Performing a Full Backup](#full-backup)

- [Performing an Incremental Backup](#incremental-backup)

- [List of Properties](#list-of-properties)

- [Restoring your Backup](#restore-backup)

- [Verify the Backup is Successful](#verify)

<a id="guidelines"></a>
## Guidelines

* Run this command during a period of low activity in your system. The backup does not block system activities, but it uses file system resources on all hosts in your distributed system and can affect performance.

* If you try to create backup files from a running system using file copy commands, you can get incomplete and unusable copies.

* Make sure the target backup directory exists and has the proper permissions for your members to write to it and create subdirectories.

* It is recommended to [compact your disk store](store-compact-disk-store.md) before running the backup.

* Make sure that those SnappyData members that host persistent data are running in the distributed system. Offline members cannot back up their disk stores. (A complete backup can still be performed if all table data is available in the running members).

<a id="backup-directory"></a>
## Specifying the Backup Directory

The directory you specify for backup can be used multiple times. Each backup first creates a top-level directory for the backup, under the directory you specify, identified to the minute.</br>
You can use one of two formats:

* Use a single physical location, such as a network file server. (For example, <_fileServerDirectory_>/<_SnappyBackupLocation_>).

* Use a directory that is local to all host machines in the system. (For example, <_SnappyBackupLocation_>).

<a id="directory-structure"></a>
## Backup Directory Structure and Contents
The backup directory contains a backup of the persistent data.  Below is the structure of files and directories backed up in a distributed system:

```pre
2018-03-15-05-31-46:
10_80_141_112_10715_ec_v0_7393 10_80_141_112_10962_v1_57099

2018-03-15-05-31-46/10_80_141_112_10715_ec_v0_7393:
config diskstores README.txt restore.sh user
2018-03-15-05-31-46/10_80_141_112_10715_ec_v0_7393/config:
2018-03-15-05-31-46/10_80_141_112_10715_ec_v0_7393/user:
2018-03-15-05-31-46/2018-03-15-05-31-46/10_80_141_112_10715_ec_v0_7393/diskstores/
GFXD-DD-DISKSTORE_4d9fa95e-7746-4d4d-b404-2648d64cf35e GFXD-DEFAULT-DISKSTORE_3c446ce4-43e4-4c14-bce5-e4336b6570e5

2018-03-15-05-31-46/10_80_141_112_10962_v1_57099:
config diskstores README.txt restore.sh user
2018-03-15-05-31-46/10_80_141_112_10962_v1_57099/config
2018-03-15-05-31-46/10_80_141_112_10962_v1_57099/user
2018-03-15-05-31-46/10_80_141_112_10962_v1_57099/diskstores:
GFXD-DD-DISKSTORE_76705038-10de-4b3e-955b-446546fe4036 GFXD-DEFAULT-DISKSTORE_157fa93d-c8a9-4585-ba78-9c10eb9c2ab6 USERDISKSTORE_216d5484-86f7-4e82-be81-d5bf7c2ba59f USERDISKSTORE-SNAPPY-DELTA_e7e12e86-3907-49e6-8f4c-f8a7a0d4156c
```

| Directory  | Contents                                                     |
| ---------- | ------------------------------------------------------------ |
| config     | For internal use                                             |
| diskstores | - GFXD-DD-DISKSTORE: Diskstores created for DataDictionary  </br> - GFXD-DEFAULT-DISKSTORE: The default diskstore. </br>- USERDISKSTORE: Generated for diskstores created by users using [CREATE DISKSTORE](/reference/sql_reference/create-diskstore) command.</br>- USERDISKSTORE-SNAPPY-DELTA: Created for delta regions. |
| user       | For internal use                                             |
| README.txt | The file contains information about other files in a directory. |
| restore.sh | Script that copies files back to their original locations.   |

<a id="full-backup"></a>

## Performing a Full Backup

For each member with persistent data, the backup includes:

* Disk store files for all stores containing persistent tables.

* Backup of all disk stores including the disk stores created for metadata as well as separate disk stores created for row buffer. 

* Configuration files from the member startup (snappydata.properties). These configuration files are not automatically restored, to avoid interfering with any more recent configurations. In particular, if these are extracted from a master jar file, copying the separate files into your working area could override the files in the jar.

* A restore script (restore.sh), written for the member’s operating system, that copies the files back to their original locations.

To perform a full backup:

1. [Start the cluster](../../howto/start_snappy_cluster.md).

2. [Start the snappy-shell and connect to the cluster](../../programming_guide/using_snappydata_shell.md).

3. Stop all transactions running in your distributed system, and do not execute DML statements during the backup. SnappyData does not support backing up a disk store while live transactions are taking place or when concurrent DML statements are being executed.

2. Run the backup command, providing your backup directory location.

    ```
    ./bin/snappy backup <SnappyBackupLocation> -locators=localhost:<peer-discovery-address>
    ```

3. Read the message that reports on the success of the operation.

If the operation is successful, you see a message like this:

```pre
The following disk stores were backed up:
	1f5dbd41-309b-4997-a50b-95890183f8ce [<hostname>:/<LocatorLogDirectory>/datadictionary]
	5cb9afc3-12fd-4be8-9c0c-cc6c7fdec86e [<hostname>:/<LocatorLogDirectory>]
	da31492f-3234-4b7e-820b-30c6b85c19a2 [<hostname>:/<ServerLogDirectory>/snappy-internal-delta]
	5a5d7ab2-96cf-4a73-8106-7a816a67f098 [<hostname>:/<ServerLogDirectory>/datadictionary]
	42510800-40e3-4abf-bcc4-7b7e8c5af951 [<hostname>:/<ServerLogDirectory>]
Backup successful.
```

If the operation does not succeed, a message is displayed indicating that the backup was incomplete and is noted in the ending status message. It leaves the file INCOMPLETE_BACKUP in its highest level backup directory. 
Offline members leave nothing, so you only have this message from the backup operation itself. Although offline members cannot back up their disk stores, a complete backup can be obtained if at least one copy of the data is available in a running member.

If the cluster is secure, you also need to specify all the security properties as command-line arguments to the backup command. The security properties you need to provide are the same as those mentioned in the configuration files in the **conf** directory (locators, servers or leads) when the cluster is launched.
The only difference is that any valid user can run this command. That is, the user does not have to be a snappydata cluster administrator to run the backup command.

For example:

```
./bin/snappy backup   /snappydata_backup_location/   -locators=locatorhostname:10334  -auth-provider=LDAP  -gemfirexd.auth-ldap-server=ldap://<ldap-server-host>:389/  -user=<username>  -password=<password>  -gemfirexd.auth-ldap-search-base=<search-base-values>  -gemfirexd.auth-ldap-search-dn=<search-dn-values> -gemfirexd.auth-ldap-search-pw=<password>

```
Optionally, you can encrypt the user's password first and use it in the above command to explicitly avoid putting the password in plain text in the command-line. Here is [how you can encrypt the password](https://snappydatainc.github.io/snappydata/security/specify_encrypt_passwords_conf_client/#using-encrypted-password-in-client-connections)

<a id="incremental-backup"></a>
## Performing an Incremental backup

An incremental backup saves the difference between the last backup and the current data. An incremental backup copies only operation logs that are not already present in the baseline directories for each member. For incremental backups, the restore script contains explicit references to operation logs in one or more previously-chained incremental backups. When the restore script is run from an incremental backup, it also restores the operation logs from previous incremental backups that are part of the backup chain.

If members are missing from the baseline directory because they were offline or did not exist at the time of the baseline backup, those members place full backups of all their files into the incremental backup directory.

To perform an incremental backup, execute the backup command but specify the baseline directory as well as your incremental backup directory (both can be the same directory). </br>For example:

```pre
./bin/snappy backup -baseline=<SnappyBackupLocation> <SnappyBackupLocation> -locators=<peer-discovery-address>
```

The tool reports on the success of the operation. If the operation is successful, you see a message like this:

```pre
The following disk stores were backed up:
	1f5dbd41-309b-4997-a50b-95890183f8ce [<hostname>:/<LocatorLogDirectory>/datadictionary]
	5cb9afc3-12fd-4be8-9c0c-cc6c7fdec86e [<hostname>:/<LocatorLogDirectory>]
	da31492f-3234-4b7e-820b-30c6b85c19a2 [<hostname>:/<ServerLogDirectory>/snappy-internal-delta]
	5a5d7ab2-96cf-4a73-8106-7a816a67f098 [<hostname>:/<ServerLogDirectory>/datadictionary]
	42510800-40e3-4abf-bcc4-7b7e8c5af951 [<hostname>:/<ServerLogDirectory>]
Backup successful.
```

A member that fails to complete its backup is noted in this ending status message and leaves the file INCOMPLETE_BACKUP. The next time you perform a backup operation a full backup is performed.

To make additional incremental backups, execute the same backup command described in this section by providing the incremental backup directory and the baseline directory.

<a id="list-of-properties"></a>
## List of Properties

| Option | Description |
|--------|--------|
|-baseline|The directory that contains a baseline backup used for comparison during an incremental backup. The baseline directory corresponds to the backup location you specified when the last backup was performed. (For example, a baseline directory can resemble <_fileServerDirectory_>/<_SnappyDataBackupLocation_>.). </br> An incremental backup operation backs up any data that is not already present in the specified `-baseline` directory. If the member cannot find previously backed up data or if the previously backed up data is corrupt, then command performs a full backup on that member. The command also performs a full backup if you omit the `-baseline` option. Optionally, you can provide the directory with the time stamp details, to perform an incremental backup (For example, <_fileServerDirectory_>/<_SnappyDataBackupLocation_>/<_TimeStamp_>).|
|-target-directory|The directory in which SnappyData stores the backup content. See [Specifying the Backup Directory](#backup-directory).|
|-locators|List of locators used to discover members of the distributed system. Supply all locators as comma-separated host:port values. The port is the peer-discovery-port used when starting the cluster (default 10334). This is a mandatory field. For example, `-locators=localhost:10334`|
|-bind-address|The address to which this peer binds for receiving peer-to-peer messages. By default SnappyData uses the hostname, or localhost if the hostname points to a local loopback address.|
|-J-D=|Sets Java system property. For example: `-J-Dgemfire.ack-wait-threshold=20`|
|-J|Prefix for any JVM property. For example `-J-Xmx4g`|

<a id="restore-backup"></a>
## Restoring your Backup

The restore.sh script is generated for each member in the cluster in the timestamp directory. The script (restore.sh) copies files back to their original locations.

1. Navigate to the backup subdirectory with the timestamp of the backup that you want to restore. For example, if you performed multiple incremental backups, navigate to the latest backup directory in order to restore your system to the last available backup.

2. Run each restore script on the host where the backup originated.

    	<SnappyBackupLocation>/<TimestampDirectory>/restore.sh

3. Repeat this procedure as necessary for other members of the distributed system.

4. After all disk stores have been restored, restart all members of the original cluster.

You can also do this manually:

1.  Restore your disk stores when your members are offline and the system is down.

2.  Read the restore scripts to see where the files are placed and make sure the destination locations are ready. The restore scripts do not copy over files with the same names.

3.  Run the restore scripts. Run each script on the host where the backup originated.

The restore operation copies the files back to their original location. All the disk stores including users created one and disk stores for metadata are also restored.


<a id="verify"></a>
## Verify the Backup is Successful

To ensure that your backup is successful, you can try the following options:

* Execute the `select count(*) from <TableName>;` query and verify the total number of rows.

* Verify the table details in the [Snappy Pulse UI](../../monitoring/monitoring.md#table).

* If you have done updates, you can verify to see if those specific updates are available.
