# SnappyData Configuration Files

Configuration files for locator, lead, and server should be created in the **conf** folder located in the SnappyData home directory with names **locators**, **leads**, and **servers**.

To do so, you can copy the existing template files **servers.template**, **locators.template**, **leads.template**, and rename them to **servers**, **locators**, **leads**.

These files contain the hostnames of the nodes (one per line) where you intend to start the member. You can modify the properties to configure individual members.

<a id="locator"></a>
## Configuring Locators

Locators provide discovery service for the cluster. Clients (e.g. JDBC) connect to the locator and discover the lead and data servers in the cluster. The clients automatically connect to the data servers upon discovery (upon initial connection). Cluster  members (Data servers, Lead nodes) also discover each other using the locator. It is further described [here | provide link to architecture or concepts section] 
It is recommended to configure two locators (for HA) in production using conf/locators file. The locators.template file provides some examples. 

In this file, you can specify:

* The host name on which a SnappyData locator is started.

* The startup directory where the logs and configuration files for that locator instance are located.

* SnappyData specific properties that can be passed.

-- TODO 
* provide link to all the rest of the properties that can be specified. 
* Provide configuration examples ... especially the ones that are prominently used. See Locators.template
* Shyja, we need someone like Hemant to refactor this whole section. Essentially, all the prominent properties should be described in each Locator, lead, server config. It is ok to repeat even if there are common properties. Move some of the examples from section below into each of these sections. Leave advanced properties in the "list of all config properties" section. 

Create the configuration file (**locators**) for locators in the *SnappyData_home/conf* directory.

<a id="lead"></a>
## Configuring Leads

Lead Nodes primarily runs the SnappyData managed Spark driver. There is one primary lead node at any given instance, but there can be multiple secondary lead node instances on standby for fault tolerance. Applications can run Jobs using the REST service provided by the Lead node. Most of the SQL queries are automatically routed to the Lead to be planned and executed through a scheduler. 

Create the configuration file (**leads**) for leads in the *SnappyData_home/conf* directory.

TODO
* provide link to all the rest of the properties that can be specified. 
* Provide configuration examples ... especially the ones that are prominently used. See Leads.template
* Include the prominent Spark properties and provide examples. Especially important for Lead config. 


<a id="dataserver"></a>
## Configuring Data Servers
Data Servers hosts data, embeds a Spark executor, and also contains a SQL engine capable of executing certain queries independently and more efficiently than Spark. Data servers use intelligent query routing to either execute the query directly on the node or to pass it to the lead node for execution by Spark SQL.

Create the configuration file (**servers**) for data servers in the *SnappyData_home/conf* directory.

TODO
* provide link to all the rest of the properties that can be specified. 
* Provide configuration examples ... especially the ones that are prominently used. See Leads.template

<a id="properties"></a>
## SnappyData Specific Properties

The following are the few important SnappyData properties that you can configure:

TODO: Do we need this section anymore? Simply incorporate into the above sections..

* **-peer-discovery-port**: This is a locator specific property. This is the port on which locator listens for member discovery. It defaults to 10334.

* **-client-port**: Port that a member listens on for client connections. 

* **-locators**: List of locators as comma-separated host:port values used to communicate with running locators in the system and thus discover other peers of the distributed system.</br>
The list must include all locators in use, and must be configured consistently for every member of the distributed system.

* **-dir**: SnappyData members need to have the working directory. The member working directory provides a default location for the log file, persistence, and status files for each member.<br> If not specified, SnappyData creates the member directory in *SnappyData_HomeDirectory/work*. 

* **-classpath**: Location of user classes required by the SnappyData Server. This path is appended to the current classpath.

* **-heap-size**: Set a fixed heap size and for the Java VM. 

* **-J**: Use this to configure any JVM specific property. For example. -J-XX:MaxPermSize=512m. 

* **-memory-size**: Specifies the total memory that can be used by the cluster in off-heap mode. Default value is 0 (OFF_HEAP is not used by default).

Refer to the [SnappyData properties](property_description.md) for the list of SnappyData properties for Locators, Leads and Servers.

<a id="hdfs"></a>
# HDFS with SnappyData Store

If using SnappyData store persistence to Hadoop as documented [here](http://gemfirexd.docs.pivotal.io/docs-gemfirexd/disk_storage/persist-hdfs.html), then add the [hbase jar](http://search.maven.org/#artifactdetails|org.apache.hbase|hbase|0.94.27|jar) explicitly to CLASSPATH. The jar is now packed in the product tree, so that can be used or download from Apache Maven. Then add to *conf/spark-env.sh*:

```export SPARK_DIST_CLASSPATH=</path/to/>hbase-0.94.27.jar```

Substitute the actual path for `</path/to/>` above

<a id="multi-host"></a>
## Example for Multiple-Host Configuration

TODO: move these into relevant sections above. 

Let's say you want to:

* Start two Locators (on node-a:9999 and node-b:8888), two servers (node-c and node-c) and a lead (node-l).

* Change the Spark UI port from 5050 to 9090. 

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
node-l -heap-size=4096m -spark.ui.port=9090 -locators=node-b:8888,node-a:9999 -spark.executor.cores=10
```

!!! Note:
	Configuration files are consulted when servers are started and also when they are stopped. So, it is recommended to not change the configuration files when the cluster is running. 

<a id="env-setting"></a>
## Environment Settings

Any Spark or SnappyData specific environment settings can be done by creating a snappy-env.sh or spark-env.sh in _SNAPPY_HOME/conf_. 

TODO: we need to figure out how/where we describe these various files. All this will be quite confusing to the user.  

<a id="hadoop-setting"></a>
## Hadoop Provided Settings
If you want to run SnappyData with an already existing custom Hadoop cluster like MapR or Cloudera you should download Snappy without Hadoop from the download link.
This allows you to provide Hadoop at runtime.

To do this you need to put an entry in $SNAPPY-HOME/conf/spark-env.sh as below:
```
export SPARK_DIST_CLASSPATH=$($OTHER_HADOOP_HOME/bin/hadoop classpath)
```
<a id="per-component"></a>
## Per Component Configuration 

Most of the time, components would be sharing the same properties. For example, you would want all servers to start with 4096m while leads to start with 2048m. You can configure these by specifying LOCATOR_STARTUP_OPTIONS, SERVER_STARTUP_OPTIONS, LEAD_STARTUP_OPTIONS environment variables in *conf/snappy-env.sh*. 

```bash 
$ cat conf/snappy-env.sh
SERVER_STARTUP_OPTIONS="-heap-size=4096m"
LEAD_STARTUP_OPTIONS="-heap-size=2048m"
```
<a id="command-line"></a>
## Snappy Command Line Utility

Instead of starting SnappyData members using SSH scripts, they can be individually configured and started using the command line. 

```bash 
$ bin/snappy locator start  -dir=/node-a/locator1 
$ bin/snappy server start  -dir=/node-b/server1  -locators=localhost[10334]
$ bin/snappy leader start  -dir=/node-c/lead1  -locators=localhost[10334]

$ bin/snappy locator stop -dir=/node-a/locator1
$ bin/snappy server stop -dir=/node-b/server1
$ bin/snappy leader stop -dir=/node-c/lead1
```

Refer to the [SnappyData properties](property_description.md) for the list of SnappyData properties for Locators, Leads and Servers.

<a id="logging"></a>
## Logging 

Currently, log files for SnappyData components go inside the working directory. To change the log file directory, you can specify a property _-log-file_ as the path of the directory. </br>
The logging levels can be modified by adding a *conf/log4j.properties* file in the product directory. 

```bash
$ cat conf/log4j.properties 
log4j.logger.org.apache.spark.scheduler.DAGScheduler=DEBUG
log4j.logger.org.apache.spark.scheduler.TaskSetManager=DEBUG
```

!!! Note:
	For a set of applicable class names and default values see the file **conf/log4j.properties.template**, which can be used as a starting point. Consult the [log4j 1.2.x documentation](http://logging.apache.org/log4j/) for more details on the configuration file.

<a id="ssh"></a>
## Configuring SSH Login without Password

<TODO> Shouldn't be the first thing we mention before configuring Locators, leads ....?

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

<a id="ssl"></a>
## SSL Setup for Client-Server
<TODO> is this section in the right place? is there a JDBC/ODBC section? Some section for security ?

SnappyData store now has support for Thrift protocol that provides functionality equivalent to JDBC/ODBC protocols and can be used to access the store from other languages that are not yet supported directly by SnappyData. In the command-line, SnappyData locators and servers accept the `-thrift-server-address` and -`thrift-server-port` arguments to start a Thrift server.

The thrift servers use the Thrift Compact Protocol by default which is not SSL enabled. When using the snappy-start-all.sh script, these properties can be specified in the *conf/locators* and *conf/servers* files in the product directory like any other locator/server properties.

In the *conf/locators* and *conf/servers* files, you need to add `-thrift-ssl` and required SSL setup in `-thrift-ssl-properties`. Refer to the [SnappyData thrift properties](property_description.md#thrift-properties) section for more information.

From the Snappy SQL shell:

```bash
snappy> connect client 'host:port;ssl=true;ssl-properties=...';
```
For JDBC use the same properties (without the "thrift-" prefix) like:

```
jdbc:snappydata://host:port/;ssl=true;ssl-properties=...
```
For example:

For a self-signed RSA certificate in keystore.jks, you can have the following configuration in the *conf/locators* and *conf/servers* files:

```
localhost -thrift-ssl=true -thrift-ssl-properties=keystore=keystore.jks,keystore-password=password,protocol=TLS,enabled-protocols=TLSv1:TLSv1.1:TLSv1.2,cipher-suites=TLS_RSA_WITH_AES_128_CBC_SHA:TLS_RSA_WITH_AES_256_CBC_SHA:TLS_RSA_WITH_AES_128_CBC_SHA256:TLS_RSA_WITH_AES_256_CBC_SHA256
```

Use the protocol/ciphers as per requirement. The corresponding setup on client-side can look like:

```bash
snappy> connect client 'localhost:1527;ssl=true;ssl-properties=truststore=keystore.jks,truststore-password=password,protocol=TLS,enabled-protocols=TLSv1:TLSv1.1:TLSv1.2,cipher-suites=TLS_RSA_WITH_AES_128_CBC_SHA:TLS_RSA_WITH_AES_256_CBC_SHA:TLS_RSA_WITH_AES_128_CBC_SHA256:TLS_RSA_WITH_AES_256_CBC_SHA256';
```
