<a id="howto-startcluster"></a>
# How to Start a SnappyData Cluster
## Starting SnappyData Cluster on a Single Machine

If you have [downloaded and extracted](../install.md) the SnappyData product distribution, navigate to the SnappyData product root directory.

**Start the Cluster**: Run the `./sbin/snappy-start-all.sh` script to start the SnappyData cluster on your single machine using default settings. This starts a lead node, a locator, and a data server.

```pre
$ ./sbin/snappy-start-all.sh
```

It may take 30 seconds or more to bootstrap the entire cluster on your local machine.

**Sample Output**: The sample output for `snappy-start-all.sh` is displayed as:

```pre
Starting SnappyData Locator using peer discovery on: localhost[10334]
Starting DRDA server for SnappyData at address localhost/127.0.0.1[1527]
Logs generated in /home/user/snappyData/work/localhost-locator-1/snappylocator.log
SnappyData Locator pid: 9368 status: running
Starting SnappyData Server using locators for peer discovery: user1-laptop[10334]
Starting DRDA server for SnappyData at address localhost/127.0.0.1[1527]
Logs generated in /home/user1/snappyData/work/localhost-server-1/snappyserver.log
SnappyData Server pid: 9519 status: running
  Distributed system now has 2 members.
  Other members: localhost(9368:locator)<v0>:16944
Starting SnappyData Leader using locators for peer discovery: user1-laptop[10334]
Logs generated in /home/user1/snappyData/work/localhost-lead-1/snappyleader.log
SnappyData Leader pid: 9699 status: running
  Distributed system now has 3 members.
  Other members: localhost(9368:locator)<v0>:16944, 192.168.63.1(9519:datastore)<v1>:46966
```

## Starting the SnappyData Cluster on Multiple Hosts

To start the cluster on multiple hosts:

1. The easiest way to run SnappyData on multiple nodes is to use a shared file system such as NFS on all the nodes.</br> You can also extract the product distribution on each node of the cluster. If all nodes have NFS access, install SnappyData on any one of the nodes.

2. Create the configuration files using the templates provided in the **conf** folder. Copy the existing template files (**servers.template**, **locators.template** and **leads.template**) and rename them to **servers**, **locators**, **leads**.
</br> Edit the files to include the hostnames on which to start the server, locator, and lead. Refer to the [configuration](../configuring_cluster/configuring_cluster.md) section for more information on properties.

3. Start the cluster using `./sbin/snappy-start-all.sh`. SnappyData starts the cluster using SSH.

!!! Note
	It is recommended that you set up passwordless SSH on all hosts in the cluster. Refer to the documentation for more details on [installation](../install/install_on_premise.md) and [cluster configuration](../configuring_cluster/configuring_cluster.md).

<a id="start-component"></a>
## Starting Individual Components

Instead of starting SnappyData cluster using the `snappy-start-all.sh` script, individual components can be started on a system locally using the following commands:

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
