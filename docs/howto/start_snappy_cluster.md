<a id="howto-startcluster"></a>
# How to Start a TIBCO ComputeDB Cluster

## Starting TIBCO ComputeDB Cluster on a Single Machine

If you have [downloaded and extracted](../install.md) the TIBCO ComputeDB product distribution, navigate to the TIBCO ComputeDB product root directory.

**Start the Cluster**: Run the `./sbin/snappy-start-all.sh` script to start the TIBCO ComputeDB cluster on your single machine using default settings. This starts a lead node, a locator, and a data server. The Hive Thrift server also starts by default. 

```pre
$ ./sbin/snappy-start-all.sh
```

It may take 30 seconds or more to bootstrap the entire cluster on your local machine. An additional 10 seconds is required to start the Hive Thrift server. To avoid this additional 10 seconds, you can set the `snappydata.hiveServer.enabled` to false.

**Sample Output**: The sample output for `snappy-start-all.sh` is displayed as:

```pre
Logs generated in /home/cbhatt/TIB_compute_1.1.1_linux/work/localhost-locator-1/snappylocator.log
SnappyData Locator pid: 10813 status: running
  Distributed system now has 1 members.
  Started Thrift locator (Compact Protocol) on: localhost/127.0.0.1[1527]
Logs generated in /home/cbhatt/TIB_compute_1.1.1_linux/work/localhost-server-1/snappyserver.log
SnappyData Server pid: 11018 status: running
  Distributed system now has 2 members.
  Started Thrift server (Compact Protocol) on: localhost/127.0.0.1[1528]
Logs generated in /home/cbhatt/TIB_compute_1.1.1_linux/work/localhost-lead-1/snappyleader.log
SnappyData Leader pid: 11213 status: running
  Distributed system now has 3 members.
  Starting hive thrift server (session=snappy)
  Starting job server on: 0.0.0.0[8090]
```



## Starting the TIBCO ComputeDB Cluster on Multiple Hosts

To start the cluster on multiple hosts:

1. The easiest way to run TIBCO ComputeDB on multiple nodes is to use a shared file system such as NFS on all the nodes.</br> You can also extract the product distribution on each node of the cluster. If all nodes have NFS access, install TIBCO ComputeDB on any one of the nodes.

2. Create the configuration files using the templates provided in the **conf** folder. Copy the existing template files (**servers.template**, **locators.template** and **leads.template**) and rename them to **servers**, **locators**, **leads**.
</br> Edit the files to include the hostnames on which to start the server, locator, and lead. Refer to the [configuration](../configuring_cluster/configuring_cluster.md) section for more information on properties.

3. Start the cluster using `./sbin/snappy-start-all.sh`. TIBCO ComputeDB starts the cluster using SSH.

!!! Note
	It is recommended that you set up passwordless SSH on all hosts in the cluster. Refer to the documentation for more details on [installation](../install/install_on_premise.md) and [cluster configuration](../configuring_cluster/configuring_cluster.md).

## Starting Individual Components

Instead of starting TIBCO ComputeDB cluster using the `snappy-start-all.sh` script, individual components can be started on a system locally using the following commands:

!!! Tip
	All [configuration parameters](../configuring_cluster/configuring_cluster.md) are provided as command line arguments rather than reading from a configuration file.

For example, you can run any of the following commands depending on the individual component that you want to start.

```pre
$ ./bin/snappy locator start  -dir=/node-a/locator1
$ ./bin/snappy server start  -dir=/node-b/server1  -locators=localhost[10334] -heap-size=16g
$ ./bin/snappy leader start  -dir=/node-c/lead1  -locators=localhost[10334] -spark.executor.cores=32
```
!!!Note
	The path mentioned for `-dir` should exist. Otherwise, the command will fail with **FileNotFoundException**.

## Executing Commands on Selected Cluster Component 

### Syntax

```
cluster-util.sh (--on-locators|--on-servers|--on-leads|--on-all) [-y] (--copy-conf | --run "<cmd-to-run-on-selected-nodes>")

```

### Description

You can use the cluster-util.sh utility to execute a given command on selected members of the cluster. The script relies on the entries you specify in locators, servers, and leads files in the **conf** directory to identify the members of the cluster.

 *	`-on-locators` </br>
	If specified, the given command is executed on locators.
 
 *	`--on-servers `</br>
	If specified, the given command is executed on servers.
    
*	`--on-leads` </br>
    If specified, the given command is executed on leads.
    
*	`--on-all `</br>
	If specified, the given command is executed on all the member in the cluster.
 	 
*	`-y`</br>
 	If specified, the script does not prompt for the confirmation to execute the command on each member node.

 *	`--copy-conf`</br>
	This is a shortcut command. When you specify this comand, the  log4j.properties, snappy-env.sh and spark-env.sh configuration files are copied from the local machine to all the members. 	These files are copied only in the following conditions:
    *	If these are absent in the destination member
    *	If their content is different.
    	In latter case, a backup of the file is taken in **conf/backup** directory, on the destination member, before copy.
 	
*	`--run <cmd-to-run-on-selected-nodes>`
	If specified, the given command(s) is executed on specified members. Command to be executed specified after --run`` must be in double-quotes.

### Example

```
// To copy configuration files on all servers

./sbin/cluster-util.sh -on-servers   --run -copyconf”
      2.  To run “ls” command on all servers with -y option
           “./sbin/cluster-util.sh -on-servers -y  --run ls  

```
