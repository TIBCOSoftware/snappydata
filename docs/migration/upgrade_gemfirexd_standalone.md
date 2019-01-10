# Manually Upgrading from GemFire XD 1.4.x to SnappyData RowStore 1.0.0

In this document, you can find information on how to manually upgrade GemFire XD 1.4.x to SnappyData 1.0.0/RowStore 1.6. It is a step-by-step guide for configuring and customizing your system. This guide assumes you have a basic understanding of your Linux system.

* [Prerequisites](#pre-req)

* [Upgrade Process](#upgrade)

## Prerequisites<a id="pre-req"></a>

-   Download the distribution file from the [SnappyData Release page (snappydata-1.0.0)](https://github.com/SnappyDataInc/snappydata/releases)

-   Ensure that before moving into production, you have tested your development systems thoroughly with the new version

## Upgrade Process<a id="upgrade"></a>

1. [Pre-Upgrade Tasks](#Step1)

2. [Downloading and Extracting Product Distribution](#Step2)

3. [Starting the Upgrade](#Step3)

4. [Verifying Upgrade is Successful](#Step4)

<a id="Step1"></a>
### Step1: Pre-Upgrade Tasks
#### Shutting the System 
Use the `shut-down-all` and `locator stop` commands to stop all members. 
For example: 

```pre
$ gfxd shut-down-all -locators=localhost[10101]
$ gfxd locator stop -dir=$HOME/locator
```

#### Backing Up all Data Directories 
In this step, you create a backup of all the data directories.
For example: 

```pre
$cp -R node1-server /backup/snappydata/
$cp -R node2-server /backup/snappydata/
```

Here `node1-server` and `node2-server` are the data directories of GemFire XD servers.

<a id="Step2"></a>
### Step 2: Downloading and Extracting Product Distribution
1. Download the distribution binary file (snappydata-1.0.0/RowStore1.6.0) from the [SnappyData Release page](https://github.com/SnappyDataInc/snappydata/releases/)(if not done already), and extract the contents of the file to a suitable location on your computer.
For example for Linux:

		$ unzip snappydata-1.0.0-bin.zip -d <path_to_product>

	Here the <path_to_product> is the directory where you want to install the product. Repeat this step to install RowStore on each different computer where you want to run a SnappyData RowStore member. Alternatively, SnappyData RowStore can also be installed on an NFS location accessible to all members.

2. If the `PATH` variable is set to the path of GemFire XD, ensure that you update it to the `snappydata-1.0.0-bin`. Add the SnappyData RowStore **bin** and **sbin** directories to your path, as described below:

	    $ export PATH=$PATH:/path_to_product/snappydata-1.0.0-bin/bin:/path_to_product/snappydata-1.0.0-bin/sbin

    
<a id="Step3"></a>
### Step 3: Starting the Upgrade

* [Setting up Passwordless SSH](#Passwordless)

* [Creating configuration files for SnappyData servers and locators](#Config)

* [Starting the SnappyData RowStore cluster](#Start)

<a id="Passwordless"></a>
#### Setting up Passwordless SSH 
SnappyData provides utility scripts to start/stop cluster (located in the **sbin** directory of the product distribution). These scripts use SSH to execute commands on different machines on which SnappyData servers are to be started/stopped. It is recommended that you setup passwordless SSH on the system where these scripts are executed. For more information see [Configuring SSH Login without Password](../reference/misc/passwordless_ssh.md).


<a id="Config"></a>
#### Creating configuration files for SnappyData servers and locators
In this step, you create configuration files for servers and locators and then use those to start the SnappyData RowStore cluster (locators and servers) using the backed up data directories.

If the NFS location is not accessible to all members, you need to follow the process of each machine. If passwordless SSH is set up, the cluster can be started from one host.

These configuration files contain hostname of the node where a locator/server is to be started along with startup properties for it.

**To configure the locators:**

1. In the **<path_to_product>/snappydata-1.0.0-bin/conf** directory, make a copy of the **locators.template**, and rename it to **locators**.

2. Edit the locator file to set it to the previous location of the GemFire XD locator (using the -dir option)

		localhost -dir=<log-dir>/snappydata/locator

	If there are two locators running on different nodes, add the line twice to set the locator directories:

        node1 -dir=<log-dir>/snappydata/node1-locator
        node2 -dir=<log-dir>/snappydata/node2-locator

**To configure the servers:**

1. In **<path_to_product>/snappydata-1.0.0-bin/conf**, make a copy of the **servers.template** file, and rename it to **servers**.
2. Edit the servers file to set it to the previous location of the GemFire XD server (using the -dir option). You can use `-server-group` option to configure server groups.

	    localhost -dir=<log-dir>/snappydata/server -server-groups=cache

    If there are two servers running on different nodes, add the line twice the set the server directories:

        node1 -dir=<log-dir>/snappydata/node1-server -server-groups=cache
    	node2 -dir=<log-dir>/snappydata/node2-server  -server-groups=cache

	!!! Note
		The `-heap-size` property replaces GemFire XD `-initial-heap` and/or `-max-heap` properties.  SnappyData no longer supports the `-initial-heap` and `-max-heap properties`.

    In GemFire XD, if you are using a properties file (**gemfirexd.properties**) for configuring various properties such as authentication, please rename the file to **snappydata.properties**. Also, the property prefix has been changed to **snappydata** instead of **gemfirexd**.

    Alternatively, you may add these properties to above-mentioned **conf/locators** and **conf/servers** files in front of each hostname.
    
<a id="Start"></a>
### Starting the SnappyData RowStore Cluster 
After creating the configuration files start SnappyData RowStore using the following command:

```pre
snappy-start-all rowstore
```

The above command starts the SnappyData servers and locators with properties as mentioned in the **conf/servers** and **conf/locators** files. The `rowstore` option instructs SnappyData not to start the lead node, which is used when SnappyData is used for SnappyData specific extensions (like column store, running spark jobs etc.)

!!! Note
	Ensure that the distributed system is running by executing the command `snappy-status-all`.

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
### Step 4: Verifying Upgrade is Successful 
The system is started and you may fire queries on the snappy-shell to check the status of the upgrade. The `gfxd` shell is replaced by the `snappy-sql` command in SnappyData.
For example:

```pre
snappy-sql rowstore
>gfxd version 1.6.0 
gfxd> connect client '127.0.0.1:1527';
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

You can execute `show members` query to see the running members:

```pre
snappy> show members;
```

The `show members` command displays all members of the cluster (such as locators, datastores) in its output. You can execute queries on the tables (such as 'select count(*) from table') to verify the contents.


## Post-Upgrade Tasks

After you have upgraded, perform these tasks:

-  The JDBC connection string prefix has changed from `jdbc:gemfirexd` to `jdbc:snappydata` (the old `jdbc:gemfirexd` prefix works for backward compatibility reasons). Update any custom or third-party JDBC clients to use the correct connection string format.
-  Run all required applications to test that your development system is functioning correctly with the upgraded version of SnappyData.


