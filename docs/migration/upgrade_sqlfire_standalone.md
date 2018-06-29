# Manually Upgrading from SQLFire 1.1.x to SnappyData RowStore 1.6.0

In this document, you can find information on how to manually upgrade SQLFire 1.1.x to SnappyData 1.0.0/RowStore 1.6. It is a step-by-step guide for configuring and customizing your system. This guide assumes you have a basic understanding of your Linux system.

* [Prerequisites](#pre-req)

* [Upgrade Process](#upgrade-process)

<a id="pre-req"></a>
# Prerequisites

-   Download the distribution file from the [SnappyData Release page (snappydata-1.0.0)](https://github.com/SnappyDataInc/snappydata/releases)
-   Ensure that before moving into production, you have tested your development systems thoroughly with the new version

# Upgrade Process<a id="upgrade-process"></a>

1. [Pre-Upgrade Tasks](#Step1)

2. [Downloading and Extracting Product Distribution](#Step2)

3. [Starting the Upgrade](#Step3)

4. [Additional Step for Table CHAR Primary Key](#Step4)

5. [Recreating Objects](#Step5)

6. [Verifying Upgrade is Successful](#Step6)


<a id="Step1"></a>
## Step1: Pre-Upgrade Tasks
Before you stop the SQLFire cluster, you must drop the procedures, remove the loaders for all tables, stop, detach and drop the listeners. 

* [Stopping and Removing Objects](#Stopping)

* [Shutting the System](#Shutting)

* [Backing Up all Data Directories](#Backup)

<a id="Stopping"></a>
### Stopping and Removing Objects
Stop and remove the objects that depend upon the SQLFire packages (listeners, procedures and loaders etc.) 
If your system uses any stored procedures, row loaders, writers, async event listeners, or functions that reference the SQLFire package names (either `com.vmware.sqlfire` or  `com.pivotal.sqlfire`), then you must stop and remove these objects from the system before you begin the upgrade process. 

For example, if you are upgrading from SQLFire and you configured DBSynchronizer using the `built-in com.vmware.sqlfire.callbacks.DBSynchronizer` implementation, then you must remove this listener implementation.

Perform the following steps for any database objects that uses an older SQLFire (`com.vmware.sqlfire` or  `com.pivotal.sqlfire`) package name:

1. Ensure that you have the DDL commands necessary to recreate the procedures, async event listeners, row loaders and writers. Typically these commands are stored in a SQL script, used to create your database schema. 
For example, an existing DBSynchronizer might have been created using the statement:

		create asynceventlistener testlistener
		( listenerclass 'com.vmware.sqlfire.callbacks.DBSynchronizer'  initparams     'com.mysql.jdbc.Driver,jdbc:mysql://localhost:3306/gfxddb,SkipIdentityColumns=true,user=sqlfuser,secret=25325ffc3345be8888eda8156bd1c313' 
		) server groups (dbsync);

	Similarly a row loader may have been created using a command:

		call sys.attach_loader(‘schema’, ‘table’, ‘com.company.RowLoader’, null);

2. If  you do not have the DDL commands readily available, use the sqlf write-schema-to-sql command to write the existing DDL commands to the SQL file, and verify that the necessary commands are present.

3. Use `SYS.STOP_ASYNC_EVENT_LISTENER` to stop any listener or DBSynchronizer implementation that uses the older API. For example:

	    call sys.stop_async_event_listener('TESTLISTENER');

    Alter any tables that use a stopped listener or DBSynchronizer to remove the listener from the table. For example:

	    alter table mytable set asynceventlistener();

4. Use the appropriate `DROP` command (`DROP PROCEDURE`, `DROP ASYNCEVENTLISTENER`, or `DROP FUNCTION`) or system procedure (`SYS.REMOVE_LISTENER`, `SYS.REMOVE_LOADER`, `SYS.REMOVE_WRITER`) to remove procedures and listener configurations that use the older API. For example:

	    drop asynceventlistener testlistener;

    Similarly, for row loaders remove them using sys.remove_loader system procedure. For example:

	    call  sys.remove_loader(‘schema’, ‘table’)
 
5. Modify  your procedure or listener code to use the newer `com.pivotal.gemfirexd` package prefixes, and recompile as necessary. If you are using the built-in DBSynchronizer implementation, you only need to modify the DDL SQL script to specify the newer implementation in the CREATE ASYNCEVENTLISTENER command. 
    For example a new DDL can be: 

        -- Upgraded DBSynchronizer DDL
        create asynceventlistener testlistener
        (
        listenerclass 'com.pivotal.gemfirexd.callbacks.DBSynchronizer' 
        Initparams 'com.mysql.jdbc.Driver,jdbc:mysql://localhost:3306/gfxddb,SkipIdentityColumns=true,user=sqlfuser,secret=25325ffc3345be8888eda8156bd1c313' 
        )
        server groups (dbsync);

    The above DDL is executed after the upgrade is done to create the DBSynchronizer.

<a id="Shutting"></a>
### Shutting the System 
Use the shut-down-all and locator stop commands to stop all members. 
For example: 

``` pre
$ sqlf shut-down-all -locators=localhost[10101]
$ sqlf locator stop -dir=$HOME/locator
```

<a id="Backup"></a>
### Backing Up all Data Directories 
In this step, you create a backup of all the data directories.
For example: 

``` pre
$cp -R node1-server /backup/snappydata/
$cp -R node2-server /backup/snappydata/
```

Here `node1-server` and `node2-server` are the data directories of SQLFire servers.

<a id="Step2"></a>
## Step 2: Downloading and Extracting Product Distribution
1. Download the distribution binary file (snappydata-1.0.0/RowStore1.6.0) from the [SnappyData Release page](https://github.com/SnappyDataInc/snappydata/releases/) (if not done already), and extract the contents of the file to a suitable location on your computer.
For example for Linux:

	    $ unzip snappydata-1.0.0-bin.zip -d <path_to_product>

	Here the <path_to_product> is the directory where you want to install the product. Repeat this step to install RowStore on each different computer where you want to run a SnappyData RowStore member. Alternatively, SnappyData RowStore can also be installed on an NFS location that is accessible to all members.

2. If the `PATH` variable is set to the path of SQLFire, ensure that you update it to `snappydata-1.0.0-bin`. Add the SnappyData RowStore **bin **and **sbin** directories to your path, as described below:

	    $ export PATH=$PATH:/path_to_product/snappydata-1.0.0-bin/bin:/path_to_product/snappydata-1.0.0-bin/sbin

<a id="Step3"></a>
## Step 3: Starting the Upgrade

* [Setting up Passwordless SSH](#Passwordless)

* [Creating configuration files for SnappyData servers and locators](#Config)

* [Starting the SnappyData RowStore cluster](#Start)

<a id="Passwordless"></a>
### Setting up Passwordless SSH 
SnappyData provides utility scripts to start/stop cluster (located in the **sbin** directory of the product distribution). These scripts use SSH to execute commands on different machines on which SnappyData servers are to be started/stopped. It is recommended that you setup passwordless SSH on the system where these scripts are executed. For more information see [Configuring SSH Login without Password](../reference/misc/passwordless_ssh.md).

<a id="Config"></a>
### Creating configuration files for SnappyData servers and locators
In this step, you create configuration files for servers and locators, and then use those to start the SnappyData RowStore cluster (locators and servers) using the backed up data directories.

If the NFS location is not accessible to all members, you need to follow the process of each machine. If passwordless SSH is set, the cluster can be started from one host.

These configuration files contain hostname of the node where a locator/server is to be started along with startup properties for it.

**To configure the locators:**

1. In the **<path_to_product>/snappydata-1.0.0-bin/conf** directory, make a copy of the **locators.template**, and rename it to **locators**.
2. Edit the locator file to set it to the previous location of the SQLFire locator (using the `-dir` option)

	    localhost -dir=<log-dir>/snappydata/locator

    If there are two locators running on different nodes, add the line twice to set the locator directories:

        node1 -dir=<log-dir>/snappydata/node1-locator
        node2 -dir=<log-dir>/snappydata/node2-locator

**To configure the servers:**

1. In **<path_to_product>/snappydata-1.0.0-bin/conf**, make a copy of the **servers.template** file, and rename it to **servers**.
2. Edit the servers file to set it to the previous location of the SQLFire server (using the `-dir` option). You can use `-server-group` option to configure server groups.
    
	    localhost -dir=<log-dir>/snappydata/server -server-groups=cache

    If there are two servers running on different nodes, add the line twice the set the server directories:

        node1 -dir=<log-dir>/snappydata/node1-server -server-groups=cache
        node2 -dir=<log-dir>/snappydata/node2-server  -server-groups=cache

	!!! Note
    	The `-heap-size` property replaces SQLFire `-initial-heap` and/or `-max-heap` properties.  SnappyData no longer supports the `-initial-heap` and `-max-heap properties`.

    In SQLFire, if you are using a properties file (**sqlfire.properties**) for configuring various properties such as authentication, rename the file to **snappydata.properties**. Also, the property prefix has been changed to **snappydata** instead of **sqlfire**.

    Alternatively, you may add these properties to the above-mentioned **conf/locators** and **conf/servers** files in front of each hostname.

<a id="Start"></a>
### Starting the SnappyData RowStore cluster 
After creating the configuration files, start SnappyData RowStore using the following command:

```pre
snappy-start-all rowstore
```

The above command starts the SnappyData servers and locators with properties as mentioned in the **conf/servers** and **conf/locators** files. The `rowstore` option instructs SnappyData not to start the lead node, which is used when SnappyData is used for SnappyData specific extensions (like column store, running spark jobs etc.)

!!! Note
		Ensure that the distributed system is running by executing the command snappy-status-all

For example:

```pre
$snappy-status-all
SnappyData Locator pid: 13750 status: running
SnappyData Server pid: 13897 status: running
SnappyData Server pid: 13905 status: running
  Distributed system now has 2 members.
  Other members: localhost(13750:locator)<v0>:10811
SnappyData Leader pid: 0 status: stopped
```

<a id="Step4"></a>
## Step 4: Additional Step for Table CHAR Primary Key
!!! Note
	You must do this step only if you have tables with CHAR type in the primary key.

This step is necessary as the handling of blank padding of the CHAR keys has changed. If you have tables with CHAR type in primary key (single or composite), do the following for each table.

1. Create a temporary table with the same schema as the original source table:

	    snappy>create table app.member_tmp as select * from app.member with no data;

2. Insert data from the original source table into new the temporary table:

	    snappy>insert into app.member_tmp select * from app.member;


3. Truncate the original source table:

	    snappy>truncate table app.member;

4. Insert data from the temporary table to original source table:

	    snappy>insert into app.member_tmp select * from app.member;

5. Drop the temporary table:

	    snappy>drop table app.member_tmp; 
    
<a id="Step5"></a>
## Step 5: Recreating objects

In the initial steps of the upgrade, you had removed the objects that depended on `com.vmware.sqlfire` or  `com.pivotal.sqlfire`, as described in - [Stopping and Removing Objects](#Stopping).

In this step, you use the modified DDL SQL scripts to recreate stored procedures, async event listener implementations, row loaders/writers using the new `com.pivotal.gemfirexd` package names. 

1. Replace the old jar files:
    After the distributed system is running, replace the older jar containing the listener/rowloader/ writer/procedure implementations using the sqlj.replace_jar system procedure.  
    For example:

	    call sqlj.replace_jar('/home/user1/lib/tours.jar', 'app.sample1')

2. Recreate Objects:
    For example, to restore the sample DBSynchronizer configuration:

        gfxd> create asynceventlistener testlistener
        (
        listenerclass 'com.pivotal.gemfirexd.callbacks.DBSynchronizer' 
        initparams   'com.mysql.jdbc.Driver,jdbc:mysql://localhost:3306/gfxddb,SkipIdentityColumns=true,user=gfxduser,secret=25325ffc3345be8888eda8156bd1c313' 
        )
        server groups (dbsync);
        gfxd> alter table mytable set asynceventlistener (testlistener);
        gfxd> call sys.start_async_event_listener('TESTLISTENER');


    Similarly attach the row loader:

	    call sys.attach_loader(‘schema’, ‘table’, ‘com.company.RowLoader’, null’); 
    
<a id="Step6"></a>
## Step 6: Verifying Upgrade is Successful 
The system is started and you can fire queries on the snappy-shell to check the status of the upgrade. The `sql` shell is replaced by the `snappy-sql` command in SnappyData.
For example:

```pre
snappy-sql
>SnappyData version 1.0.0
snappy> connect client '127.0.0.1:1527';
Using CONNECTION0
snappy> show tables;
TABLE_SCHEM           |TABLE_NAME                     |TABLE_TYPE  |REMARKS            
-------------------------------------------------------------------------------------
SYS                   |ASYNCEVENTLISTENERS            |SYSTEM TABLE|                   
SYS                   |GATEWAYRECEIVERS               |SYSTEM TABLE|                   
SYS                   |GATEWAYSENDERS                 |SYSTEM TABLE|                   
SYS                   |SYSALIASES                     |SYSTEM TABLE|                   
SYS                   |SYSCHECKS                      |SYSTEM TABLE|                   
SYS                   |SYSCOLPERMS                    |SYSTEM TABLE|                   
SYS                   |SYSCOLUMNS                     |SYSTEM TABLE|                   
SYS                   |SYSCONGLOMERATES               |SYSTEM TABLE|                   
SYS                   |SYSCONSTRAINTS                 |SYSTEM TABLE|                   
SYS                   |SYSDEPENDS                     |SYSTEM TABLE|                   
SYS                   |SYSDISKSTORES                  |SYSTEM TABLE|                   
SYS                   |SYSFILES                       |SYSTEM TABLE|                   
SYS                   |SYSFOREIGNKEYS                 |SYSTEM TABLE|                   
SYS                   |SYSHDFSSTORES                  |SYSTEM TABLE|                   
SYS                   |SYSKEYS                        |SYSTEM TABLE|                   
SYS                   |SYSROLES                       |SYSTEM TABLE|                   
SYS                   |SYSROUTINEPERMS                |SYSTEM TABLE|                   
SYS                   |SYSSCHEMAS                     |SYSTEM TABLE|                   
SYS                   |SYSSTATEMENTS                  |SYSTEM TABLE|                   
SYS                   |SYSSTATISTICS                  |SYSTEM TABLE|                   
SYS                   |SYSTABLEPERMS                  |SYSTEM TABLE|                   
SYS                   |SYSTABLES                      |SYSTEM TABLE|                   
SYS                   |SYSTRIGGERS                    |SYSTEM TABLE|                   
SYS                   |SYSVIEWS                       |SYSTEM TABLE|                   
SYSIBM                |SYSDUMMY1                      |SYSTEM TABLE|
```          

You can execute the `show members` query to see the running members:

```pre
snappy> show members;
```

The `show members` command displays all members of the cluster (such as locators, datastores) in its output. You can execute queries on the tables (such as 'select count(*) from table') to verify the contents.


## Post-Upgrade Tasks
---------------

After you have upgraded, perform these tasks:

-  The JDBC connection string prefix has changed from `jdbc:sqlfire` to `jdbc:snappydata`. Update any custom or third-party JDBC clients to use the correct connection string format.
-  Run all required applications to test that your development system is functioning correctly with the upgraded version of SnappyData.
