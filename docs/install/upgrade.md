# Upgrade Instructions

This guide provides information for upgrading systems running an earlier version of SnappyData. We assume that you have SnappyData already installed, and you are upgrading to the latest version of SnappyData.

Before you begin the upgrade, ensure that you understand the new features and any specific requirements for that release.

## Before You Upgrade

1. Confirm that your system meets the hardware and software requirements described in [System Requirements](../install/system_requirements.md) section.

2. Backup the existing environment: </br>Create a backup of the locator, lead, and server configuration files that exist in the **conf** folder located in the SnappyData home directory.

3. Stop the cluster and verify that all members are stopped: You can shut down the cluster using the `sbin/snappy-stop-all.sh` command. </br>To ensure that all the members have been shut down correctly, use the `sbin/snappy-status-all.sh` command.

4. Create a [backup of the operational disk store files](../reference/command_line_utilities/store-backup.md) for all members in the distributed system.

5. Reinstall SnappyData: After you have stopped the cluster, [install the latest version of SnappyData](../install/index.md).

6. Reconfigure your cluster using the locator, lead, and server configuration files you backed up in step 1.

7. To ensure that the restore script (restore.sh) copies files back to their original locations, make sure that the disk files are available at the original location before restarting the cluster with the latest version of SnappyData.

## Upgrading to SnappyData 1.3.1 from 1.0.1 or Earlier Versions

When you upgrade to SnappyData 1.3.1 from product version 1.0.1 or earlier versions, it is recommended to save all the table data as parquet files, recreate the tables in the new cluster, and then load data from the saved parquet files. Before taking the backup ensure that no operations are currently running on the system. Ensure to cleanup the data from the previous cluster and start the new cluster from a clean directory.

For example:

```
# Creating parquet files in older product version 1.0.1 or prior:
snappy> create external table table1Parquet using parquet options (path '<path-to-parquet-file>') as select * from table1;
snappy> drop table table1;
snappy> drop table table1Parquet;

# Creating tables from parquet files in SnappyData 1.3.1
snappy> create external table table1_parquet using parquet options (path '<path-to-parquet-file>') ;
snappy> create table table1(...);
snappy> insert into table1 select * from table1_parquet;
```
Use a path for the Parquet file that has enough space to hold the table data. Once the re-import has completed successfully, make sure that the Parquet files are deleted explicitly.

!!! Note
	Upgrade to SnappyData from 1.0.2 or later versions can be done directly.
	For changes related to Log4j 2 migration in 1.3.1, please check the release notes.


## Migrating to the SnappyData ODBC Driver

If you have been using TIBCO ComputeDB™ ODBC Driver from 1.2.0 or earlier releases, then you can install the
SnappyData ODBC Driver alongside it without any conflicts. The product name as well as the ODBC Driver name
for the two are `TIBCO ComputeDB` and `SnappyData` respectively that do not overlap with one another.
The older drivers and their DSNs will continue to work against SnappyData 1.3.1 without any changes.

While the older driver will continue to work, migrating to the new SnappyData ODBC Driver is highly recommended
to avail the new features and bug fixes over the TIBCO ComputeDB™ ODBC Driver releases. This includes new options
`AutoReconnect`, `CredentialManager` and `DefaultSchema`, new APIs SQLCancel/SQLCancelHandle,
fixes to APIs like SQLPutData, SQLBindParameter, SQLGetDiagRec, SQLGetDiagField and SQLGetInfo, updated dependencies
among others. See the [Release Notes](../release_notes/release_notes.md#odbc-changes) for more details.

Migration to the SnappyData ODBC Driver will involve installation from the new MSI installer
(see [docs](../howto/connect_using_odbc_driver.md)), then creating new DSNs that have their keys and values
copied from the previous ones. While just changing the DSN driver to `SnappyData` in the registry will work,
it is recommended to create it afresh with the ODBC Setup UI that will allow setting the new options easily.
