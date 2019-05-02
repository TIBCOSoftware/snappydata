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

## Upgrading to TIBCO ComputeDB 1.1.0 from Earlier Versions

The following steps should be followed to upgrade from the previous versions (1.0.1, 1.0.2, 1.0.2.1) of the product:

1.	In addition to any existing configuration options, add `-J-Dsnappydata.DISALLOW_METASTORE_ON_LOCATOR=true` property to the **conf/locators** files for each of the locator as shown:

        conf/locators
        locator1 -dir=<path1> -peer-discovery-port=<port1> -locators=<locator2:port2> -J-Dsnappydata.DISALLOW_METASTORE_ON_LOCATOR=true
        locator2 -dir=<path2> -peer-discovery-port=<port2> -locators= <locator1:port1> -J-Dsnappydata.DISALLOW_METASTORE_ON_LOCATOR=true

2.	[Start the cluster](/howto/start_snappy_cluster.md) and verify that cluster is up and running. Also verify that all the tables are present along with their data. 
3.	In the existing running cluster, start one more locator without the given system property and ensure to provide `-locators=oldlocator:port` property in the conf files for the locator as shown in the following example. In this example configuration below locator3 is added apart from locator1 and locator2. Also locator3 does not specify the property '-J-Dsnappydata.DISALLOW_METASTORE_ON_LOCATOR=true`. 
		      
        locator1 -dir=<path1> -peer-discovery-port=<port1> -locators=<locator2:port2> -J-Dsnappydata.DISALLOW_METASTORE_ON_LOCATOR=true
        locator2 -dir=<path2> -peer-discovery-port=<port2> -locators= <locator1:port1> -J-Dsnappydata.DISALLOW_METASTORE_ON_LOCATOR=true
        locator3 -dir=<path3> -peer-discovery-port=<port3> -locators= <locator1:port1> ,<locator2:port2>
        
        Now start the cluster:
        ./sbin/snappy-start-all.sh
  
5.	Shutdown the old locators that were started with the system property, using the commands as shown:

        ./bin/snappy locator stop -dir=-dir=<path1>
        ./bin/snappy locator stop -dir=-dir=<path2>
      
6.	Remove the directory contents from the stopped locators and start those locators without this property:</br> `-J-Dsnappydata.DISALLOW_METASTORE_ON_LOCATOR=true` as shown:

		Remove the old locator directory contents as follows:
		rm -rf <path1>/*
        rm -rf <path2>/*
        
        Edit the conf/locators file as follows:
        locator1 -dir=<path1> -peer-discovery-port=<port1> -locators=<locator2:port2>,<locator3:port3>
 		locator2 -dir=<path2> -peer-discovery-port=<port2> -locators=<locator1:port1>,<locator3:port3>
 		locator3 -dir=<path3> -peer-discovery-port=<port3> -locators=<locator1:port1>,<locator2:port2>
        
        Now start the cluster:
        ./sbin/snappy-start-all.sh

7.	Verify the tables and data.


The following steps must be followed to upgrade from version 1.0.0 of the product:

For best performance, it is recommended that you recreate any large column tables after you upgrade from 1.0.0 to 1.0.1/1.0.2. The following two improvements provided in 1.0.1 take effect:

* Compression for on-disk and over the network data.

* Separate disk-store for column delta store. This improves the compactor performance significantly for cases of frequent JDBC/ODBC inserts or small inserts where the delta store gets used frequently.

!!! Note

	Ensure that no operations are currently running on the system.

The following example demonstrates how you can re-create your column tables using a Parquet-based external table:

```pre
snappy> create external table table1Parquet using parquet options (path '...') as select * from table1;
```

```pre
snappy> drop table table1;
```

```pre
snappy> create table table1 ...;
```

```pre
snappy> insert into table1 as select * from table1Parquet;
```

```pre
snappy> drop table table1Parquet;
```

!!! Note

​	 Use a path for the Parquet file that has enough space to hold the table data. Once the re-import has completed successfully, make sure that the Parquet files are deleted explicitly.
