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
