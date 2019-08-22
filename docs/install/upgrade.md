# Upgrade Instructions

This guide provides information for upgrading systems running an earlier version of TIBCO ComputeDB. It is assumed that you have already installed TIBCO ComputeDB and you are upgrading to the latest version.

Before you begin the upgrade, ensure that you understand the new features and any specific requirements for that release.

## Before You Upgrade

1. Confirm that your system meets the hardware and software requirements described in [System Requirements](../install/system_requirements.md) section.

2. Backup the existing environment: </br>Create a backup of the locator, lead, and server configuration files that exist in the **conf** folder located in the TIBCO ComputeDB home directory.

3. Stop the cluster and verify that all members are stopped: You can shut down the cluster using the `sbin/snappy-stop-all.sh` command. </br>To ensure that all the members have been shut down correctly, use the `sbin/snappy-status-all.sh` command.

4. Create a [backup of the operational disk store files](../reference/command_line_utilities/store-backup.md) for all members in the distributed system.

5. Reinstall TIBCO ComputeDB: After you have stopped the cluster, [install the latest version of TIBCO ComputeDB](../install.md).

6. Reconfigure your cluster using the locator, lead, and server configuration files you backed up in step 1.

7. To ensure that the restore script (restore.sh) copies files back to their original locations, make sure that the disk files are available at the original location before restarting the cluster with the latest version of TIBCO ComputeDB.

## Upgrading to TIBCO ComputeDB 1.1.1 from 1.0.1 or Earlier Versions

When you upgrade to TIBCO ComputeDB 1.1.1 from product version 1.0.1 or earlier versions, it is recommended to save all the table data as parquet files, recreate the tables in the new cluster, and then load data from the saved parquet files. Before taking the backup ensure that no operations are currently running on the system. Ensure to cleanup the data from the previous cluster and start the new cluster from a clean directory. 

For example:

```
# Creating parquet files in older product version 1.0.1 or prior:
snappy> create external table table1Parquet using parquet options (path '<path-to-parquet-file>') as select * from table1;
snappy> drop table table1;
snappy> drop table table1Parquet;

# Creating tables from parquet files in Tibco ComputeDB 1.1.1
snappy> create external table table1_parquet using parquet options (path '<path-to-parquet-file>') ;
snappy> create table table1(...);
snappy> insert into table1 select * from table1_parquet;
```
Use a path for the Parquet file that has enough space to hold the table data. Once the re-import has completed successfully, make sure that the Parquet files are deleted explicitly.

!!! Note
	Upgrade to TIBCO ComputeDB from 1.0.2 or later versions can be done directly. 
