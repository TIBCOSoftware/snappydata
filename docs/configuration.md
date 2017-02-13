#Overview
SnappyData, a database server cluster, has three main components - Locator, Server, and Lead.

The Lead node embeds a Spark driver and the Server node embeds a Spark Executor. The server node also embeds a SnappyData store.

SnappyData cluster can be started with the default configurations using script `sbin/snappy-start-all.sh`. This script starts up a locator, one data server, and one lead node. However, SnappyData can be configured to start multiple components on different nodes. 
Also, each component can be configured individually using configuration files. In this document, 
we discuss how the components can be individually configured. We also discuss various other configurations of SnappyData.

## Configuration Files

Configuration files for locator, lead, and server should be created in the **conf** folder located in the SnappyData home directory with names **locators**, **leads**, and **servers**.

To do so, you can copy the existing template files **servers.template**, **locators.template**, **leads.template**, and rename them to **servers**, **locators**, **leads**.

These files contain the hostnames of the nodes (one per line) where you intend to start the member. You can modify the properties to configure individual members.

#### Configuring Locators

Locators provide discovery service for the cluster. It informs a new member joining the group about other existing members. A cluster usually has more than one locator for high availability reasons.

In this file, you can specify:

* The host name on which a SnappyData locator is started.

* The startup directory where the logs and configuration files for that locator instance are located.

* SnappyData specific properties that can be passed.

Create the configuration file (**locators**) for locators in the *SnappyData_home/conf* directory.

#### Configuring Leads

Lead Nodes act as a Spark driver by maintaining a singleton SparkContext. There is one primary lead node at any given instance, but there can be multiple secondary lead node instances on standby for fault tolerance. The lead node hosts a REST server to accept and run applications. The lead node also executes SQL queries routed to it by “data server” members.

Create the configuration file (**leads**) for leads in the *SnappyData_home/conf* directory.

#### Configuring Data Servers
Data Servers hosts data, embeds a Spark executor, and also contains a SQL engine capable of executing certain queries independently and more efficiently than Spark. Data servers use intelligent query routing to either execute the query directly on the node or to pass it to the lead node for execution by Spark SQL.

Create the configuration file (**servers**) for data servers in the *SnappyData_home/conf* directory.

#### SnappyData Specific Properties

The following are the few important SnappyData properties that you can configure:

* **-peer-discovery-port**: This is a locator specific property. This is the port on which locator listens for member discovery. It defaults to 10334. 

* **-client-port**: Port that a member listens on for client connections. 

* **-locators**: List of other locators as comma-separated host:port values. For locators, the list must include all other locators in use. For Servers and Leads, the list must include all the locators of the distributed system.

* **-dir**: SnappyData members need to have the working directory. The member working directory provides a default location for the log file, persistence, and status files for each member.<br> If not specified, SnappyData creates the member directory in *SnappyData_HomeDirectory/work*. 

* **-classpath**: This can be used to provide any application specific code to the lead and servers. We envisage having setJar like functionality going forward but for now, the application jars have to be provided during startup. 

* **-heap-size**: Set a fixed heap size and for the Java VM. 

* **-J**: Use this to configure any JVM specific property. For example. -J-XX:MaxPermSize=512m. 

For a detailed list of SnappyData configurations for Leads and Servers, you can consult [this](http://gemfirexd.docs.pivotal.io/docs-gemfirexd/reference/gfxd_commands/gfxd-server.html). For a detailed list of SnappyData configurations for Locators, you can consult [this](http://gemfirexd.docs.pivotal.io/docs-gemfirexd/reference/gfxd_commands/gfxd-locator.html).

## HDFS with SnappyData Store

If using SnappyData store persistence to Hadoop as documented [here](http://gemfirexd.docs.pivotal.io/docs-gemfirexd/disk_storage/persist-hdfs.html), then add the [hbase jar](http://search.maven.org/#artifactdetails|org.apache.hbase|hbase|0.94.27|jar) explicitly to CLASSPATH. The jar is now packed in the product tree, so that can be used or download from Apache Maven. Then add to conf/spark-env.sh:

```export SPARK_DIST_CLASSPATH=</path/to/>hbase-0.94.27.jar```

Substitute the actual path for `</path/to/>` above

## Spark Specific Properties 

Since SnappyData embeds Spark components, [Spark Runtime environment properties](http://spark.apache.org/docs/latest/configuration.html#runtime-environment) (like  spark.driver.memory, spark.executor.memory, spark.driver.extraJavaOptions, spark.executorEnv) do not take effect. They have to be specified using SnappyData configuration properties. 

Apart from these properties, other Spark properties can be specified in the configuration file of the Lead nodes. You have to prefix them with a _hyphen(-)_. The Spark properties that are specified on the Lead node are sent to the Server nodes. Any Spark property that is specified in the conf/servers or conf/locators file is ignored. 

!!! Note
	Currently we do not honor properties specified using spark-config.sh. </Note>

## Example for Multiple-Host Configuration

Let's say you want to:

* Start two Locators (on node-a:9999 and node-b:8888), two servers (node-c and node-c) and a lead (node-l).

* Change the Spark UI port from 4040 to 9090. 

* Set spark.executor.cores as 10 on all servers. 

The following can be your conf files. 

```bash
$ cat conf/locators
node-a -peer-discovery-port=9999 -dir=/node-a/locator1 -heap-size=1024m -locators=node-b:8888
node-b -peer-discovery-port=8888 -dir=/node-b/locator2 -heap-size=1024m -locators=node-a:9999

$ cat conf/servers
node-c -dir=/node-c/server1 -heap-size=4096m -locators=node-b:8888,node-a:9999
node-c -dir=/node-c/server2 -heap-size=4096m -locators=node-b:8888,node-a:9999

$ cat conf/leads
# This goes to the default directory 
node-l -heap-size=4096m -J-XX:MaxPermSize=512m -spark.ui.port=9090 -locators=node-b:8888,node-a:9999 -spark.executor.cores=10
```
!!! Note
	Conf files are consulted when servers are started and also when they are stopped. So, we do not recommend changing the conf files while the cluster is running. </Note>

## Environment Settings

Any Spark or SnappyData specific environment settings can be done by creating a snappy-env.sh or spark-env.sh in _SNAPPY_HOME/conf_. 

## Hadoop Provided Settings
If you want to run SnappyData with an already existing custom Hadoop cluster like MapR or Cloudera you should download Snappy without Hadoop from the download link.
This allows you to provide Hadoop at runtime.

To do this you need to put an entry in $SNAPPY-HOME/conf/spark-env.sh as below:
```
export SPARK_DIST_CLASSPATH=$($OTHER_HADOOP_HOME/bin/hadoop classpath)
```

## Per Component Configuration 

Most of the time, components would be sharing the same properties. For example, you would want all servers to start with 4096m while leads to start with 2048m. You can configure these by specifying LOCATOR_STARTUP_OPTIONS, SERVER_STARTUP_OPTIONS, LEAD_STARTUP_OPTIONS environment variables in conf/snappy-env.sh. 

```bash 
$ cat conf/snappy-env.sh
SERVER_STARTUP_OPTIONS="-heap-size=4096m"
LEAD_STARTUP_OPTIONS="-heap-size=2048m"
```

## Add Servers to the Cluster and Stop Servers
RowStore manages data in a flexible way that enables you to expand or contract your cluster at runtime to support different loads. To dynamically add more capacity to a cluster, you add new server members and specify the -`rebalance` option.

1. Open a new terminal or command prompt window, and create a directory for the new server:

        $ cd ~
        $ mkdir server3        

2. When you add a new server to the cluster, you can specify the -rebalance option to move partitioned table data buckets between host members as needed to establish the best balance of data across the distributed system. (See Rebalancing Partitioned Data on RowStore Members for more information.) Start the new server to see rebalancing in action:

        $ snappy-shell rowstore server start -dir=$HOME/server3 -locators=localhost[10101] -client-port=1530 -enable-network-partition-detection=true -rebalance
        Starting RowStore Server using locators for peer discovery: localhost[10101]
        Starting network server for RowStore Server at address localhost/127.0.0.1[1530]
        Logs generated in /home/gpadmin/server3/snappyserver.log
        RowStore Server pid: 41165 status: running
          Distributed system now has 4 members.
          Other members: 192.168.125.147(39381:locator)<v0>:50344, 192.168.125.147(40612:datastore)<v5>:11337, 192.168.125.147(40776:datastore)<v6>:31019

3. View the contents of the new RowStore directory:

        $ ls server3
        BACKUPGFXD-DEFAULT-DISKSTORE_1.crf  DRLK_IFGFXD-DEFAULT-DISKSTORE.lk
        BACKUPGFXD-DEFAULT-DISKSTORE_1.drf  snappyserver.log
        BACKUPGFXD-DEFAULT-DISKSTORE.if     start_snappyserver.log
        datadictionary

4. You can view all members of the distributed system using `snappy-shell.` Return to the `snappy-shell` session window and execute the query:

        snappy> select id from sys.members;
        ID                                                                             
        -------------------------------------------------------------------------------
        192.168.125.147(40612)<v5>:11337                                               
        192.168.125.147(39381)<v0>:50344                                               
        192.168.125.147(40776)<v6>:31019                                               
        192.168.125.147(41165)<v7>:35662                                               
        
        4 rows selected

5. Verify that all servers now host the data:

        snappy> select distinct dsid() as id from flights;
        ID                                                                             
        -------------------------------------------------------------------------------
        192.168.125.147(40612)<v5>:11337                                               
        192.168.125.147(40776)<v6>:31019                                               
        192.168.125.147(41165)<v7>:35662                                               
        
        3 rows selected

6. Examine the table data that each server hosts:

        snappy> select count(*) memberRowCount, dsid() from flights group by dsid();
        MEMBERROWCOUNT|2                                                               
        -------------------------------------------------------------------------------
        150           |192.168.125.147(40612)<v5>:11337                                
        114           |192.168.125.147(40776)<v6>:31019                                
        278           |192.168.125.147(41165)<v7>:35662                                
        
        3 rows selected

7. Exit the snappy-shell session:

        snappy> exit;

8. You can stop an individual RowStore server by using the snappy-shell rowstore server stop command and specifying the server directory. To shut down all data stores at once, use the snappy-shell shut-down-all command:

        $ snappy-shell shut-down-all -locators=localhost[10101]
        Connecting to distributed system: locators=localhost[10101]
        Successfully shut down 3 members

9. After all data stores have stopped, shut down the locator as well:

        $ snappy-shell locator stop -dir=$HOME/locator
        The RowStore Locator has stopped.

## Starting Members Individually using Command Line (without scripts)

Instead of starting SnappyData members using SSH scripts, they can be individually configured and started using the command line. 

```bash 
$ bin/snappy-shell locator start  -dir=/node-a/locator1 
$ bin/snappy-shell server start  -dir=/node-b/server1  -locators:localhost:10334

$ bin/snappy-shell locator stop
$ bin/snappy-shell server stop
```
  
## Logging 

Currently, log files for SnappyData components go inside the working directory. To change the log file directory, you can specify a property _-log-file_ as the path of the directory. There is a log4j.properties file that is shipped with the jar. We recommend not to change it at this moment. However, to change the logging levels, you can create a conf/log4j.properties with the following details: 

```bash
$ cat conf/log4j.properties 
log4j.rootCategory=DEBUG, file
```

## Configuring SSH Login without Password
By default, Secure Socket Shell (SSH) requires a password for authentication on a remote server.
This setting needs to be modified to allow you to login to the remote host through the SSH protocol, without having to enter your SSH password multiple times when working with SnappyData.

To install and configure SSH, do the following:

1. **Install SSH** <br>
	To install SSH,  type:
    `sudo apt-get install ssh`
    Mac OS X has a built-in SSH client.

2. **Generate an RSA key pair**<br>
    To generate an RSA key pair run the following command on the client computer,
    `ssh-keygen -t rsa`
    Press **Enter** when prompted to enter the file in which to save the key, and for the pass phrase.

3.  **Copy the Public Key**<br>
    Once the key pair is generated, copy the contents of the public key file, to the authorized key on the remote site, by typing `cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys`


