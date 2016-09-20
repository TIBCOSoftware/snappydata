## Configuration

SnappyData, a database server cluster, has three main components - Locator, Server and Lead. Lead node embeds a Spark driver and Server node embeds a Spark Executor. Server node also embeds a snappy store. As discussed in Getting Started, SnappyData cluster can be started with default configurations using script sbin/snappy-start-all.sh. This script starts up a locator, one data server and one lead node. However, SnappyData can be configured to start multiple components on different nodes. Also, each component can be configured individually using configuration files. In this document, we discuss how the components can be individually configured. We also discuss various other configurations of SnappyData. 

### Configuration files

Configuration files for locator, lead and server should be created in SNAPPY_HOME with names as conf/locators, conf/leads and conf/servers respectively. These files contain the hostnames of the nodes (one per line) where you intend to start the member. You can specify the properties to configure individual members. 

#### SnappyData specific properties

The following are the few important SnappyData properties that you would like to configure. 

1. **_-peer-discovery-port_**: This is a locator specific property. This is the port on which locator listens for member discovery. It defaults to 10334. 
2. **_-client-port_**: Port that a member listens on for client connections. 
3. **_-locators_**: List of other locators as comma-separated host:port values. For locators, the list must include all other locators in use. For Servers and Leads, the list must include all the locators of the distributed system.
4. **_-dir_**: SnappyData members need to have working directory. The member working directory provides a default location for log, persistence, and status files for each member. If not specified, SnappyData creates the member directory in _SNAPPY_HOME\work_. 
5. **_-classpath_**: This can be used to provide any application specific code to the lead and servers. We envisage having setJar like functionality going forward but for now, the application jars have to be provided during startup. 
6. **_-heap-size_**: Set a fixed heap size and for the Java VM. 
7. **_-J_**: Use this to configure any JVM specific property. For e.g. -J-XX:MaxPermSize=512m. 

For a detail list of SnappyData configurations for Leads and Servers, you can consult [this](http://gemfirexd.docs.pivotal.io/docs-gemfirexd/reference/gfxd_commands/gfxd-server.html). For a detail list of SnappyData configurations for Locators, you can consult [this](http://gemfirexd.docs.pivotal.io/docs-gemfirexd/reference/gfxd_commands/gfxd-locator.html).

#### HDFS with SnappyData store

If using SnappyData store persistence to Hadoop as documented [here](http://gemfirexd.docs.pivotal.io/docs-gemfirexd/disk_storage/persist-hdfs.html), then you will need to add the [hbase jar](http://search.maven.org/#artifactdetails|org.apache.hbase|hbase|0.94.27|jar) explicitly to CLASSPATH. The jar is now packed in the product tree, so that can be used or download from maven. Then add to conf/spark-env.sh:

export SPARK_DIST_CLASSPATH=/path/to/hbase-0.94.27.jar

(subsitute the actual path for /path/to/ above)

#### Spark specific properties 

Since SnappyData embeds Spark components, [Spark Runtime environment properties](http://spark.apache.org/docs/latest/configuration.html#runtime-environment) (like  spark.driver.memory, spark.executor.memory, spark.driver.extraJavaOptions, spark.executorEnv) do not take effect. They have to be specified using SnappyData configuration properties. Apart from these properties, other Spark properties can be specified in the configuration file of the Lead nodes. You have to prepend them with a _hyphen(-)_. The Spark properties that are specified on the Lead node are sent to the Server nodes. Any Spark property that is specified in the conf/servers or conf/locators file is ignored. 
>#####Note:
Currently we do not honour properties specified using spark-config.sh. 

#### Example Configuration

Let's say you want to start two Locators (on node-a:9999 and node-b:8888), two servers (node-c and node-c) and a lead (node-l). Also, you would like to change the Spark UI port from 4040 to 9090. Also, you would like to set spark.executor.cores as 10 on all servers. The following can be your conf files. 
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
> ##### Note: 
conf files are consulted when servers are started and also when they are stopped. So, we do not recommend changing the conf files while the cluster is running. 

#### Environment settings

Any Spark or SnappyData specific environment settings can be done by creating a snappy-env.sh or spark-env.sh in _SNAPPY_HOME/conf_. 

##### Hadoop Provided settings
If you want run SnappyData with an already existing custom Hadoop cluster like MapR or Cloudera you should download Snappy without hadoop from the download link.
This will allow you to provide hadoop at runtime.
To do this you need to put an entry in $SNAPPY-HOME/conf/spark-env.sh an entry as below.

export SPARK_DIST_CLASSPATH=$($OTHER_HADOOP_HOME/bin/hadoop classpath)

#### Per Component Configuration 

Most of the time, components would be sharing the same properties. For e.g. you would want all servers to start with 4096m while leads to start with 2048m. You can configure these by specifying LOCATOR_STARTUP_OPTIONS, SERVER_STARTUP_OPTIONS, LEAD_STARTUP_OPTIONS environment variables in conf/snappy-env.sh. 

```bash 
$ cat conf/snappy-env.sh
SERVER_STARTUP_OPTIONS="-heap-size=4096m"
LEAD_STARTUP_OPTIONS="-heap-size=2048m"
```

### snappy-shell Command Line Utility

Instead of starting SnappyData members using ssh scripts, they can be individually configured and started using command line. 

```bash 
$ bin/snappy-shell locator start  -dir=/node-a/locator1 
$ bin/snappy-shell server start  -dir=/node-b/server1  -locators:localhost:10334

$ bin/snappy-shell locator stop
$ bin/snappy-shell server stop
```
  
### Logging 

Currently, log files for SnappyData components go inside the working directory. To change the log file directory, you can specify a property _-log-file_ as the path of the directory. There is a log4j.properties file that is shipped with the jar. We recommend not to change it at this moment. However, to change the logging levels, you can create a conf/log4j.properties with the following details: 

```bash
$ cat conf/log4j.properties 
log4j.rootCategory=DEBUG, file
```


