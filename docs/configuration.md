# Configuring the Cluster
SnappyData has three main components - Locator, Server, and Lead.

The Lead node embeds a Spark driver and the Server node embeds a Spark Executor. The server node also embeds a SnappyData store.

SnappyData cluster can be started with the default configurations using script `sbin/snappy-start-all.sh`. This script starts up a locator, one data server, one lead node and the Hive Thrift server. However, SnappyData can be configured to start multiple components on different nodes. </br>


Also, each component can be configured individually using configuration files. In this section, you can learn how the components can be individually configured and also learn about various other configurations of SnappyData.

The following topics are covered in this section:

* [Configuration Files](configuring_cluster/configuring_cluster.md#configuration-files)

	- [Configuring Locators](configuring_cluster/configuring_cluster.md#locator)

	- [Configuring Leads](configuring_cluster/configuring_cluster.md#lead)
	
	- [Configuring Data Servers](configuring_cluster/configuring_cluster.md#dataserver)

	- [Configuring SnappyData Smart Connector](configuring_cluster/configuring_cluster.md#configure-smart-connector)

	- [Environment Settings](configuring_cluster/configuring_cluster.md#environment)

	 	- [Hadoop Provided Settings](configuring_cluster/configuring_cluster.md#hadoop-setting)

	 	- [SnappyData Command Line Utility](configuring_cluster/configuring_cluster.md#command-line)

	- [Logging](configuring_cluster/configuring_cluster.md#logging)

* [List of Properties](configuring_cluster/property_description.md)

* [Firewalls and Connections](configuring_cluster/firewalls_connections.md)