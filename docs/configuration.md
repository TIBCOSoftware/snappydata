# Overview
SnappyData, a database server cluster, has three main components - Locator, Server, and Lead.

The Lead node embeds a Spark driver and the Server node embeds a Spark Executor. The server node also embeds a SnappyData store.

SnappyData cluster can be started with the default configurations using script `sbin/snappy-start-all.sh`. This script starts up a locator, one data server, and one lead node. However, SnappyData can be configured to start multiple components on different nodes. 
Also, each component can be configured individually using configuration files. In this document, 
we discuss how the components can be individually configured. We also discuss various other configurations of SnappyData.

The following topics are covered in this section:

* [Configuration Files](/../configuring_cluster/configuring_cluster#configuration-files)

	- [Configuring Locators](/../configuring_cluster/configuring_cluster#locator)

	- [Configuring Leads](/../configuring_cluster/configuring_cluster#lead)

	- [Configuring Data Servers](/../configuring_cluster/configuring_cluster#dataserver)

	- [SnappyData Specific Properties](/../configuring_cluster/configuring_cluster#properties) </br>

* [HDFS with SnappyData Store](/../configuring_cluster/configuring_cluster#hdfs)

* [Example for Multiple-Host Configuration](/../configuring_cluster/configuring_cluster#multi-host)

* [Environment Settings](/../configuring_cluster/configuring_cluster#env-setting)

* [Hadoop Provided Settings](/../configuring_cluster/configuring_cluster#hadoop-setting)

* [Per Component Configuration](/../configuring_cluster/configuring_cluster#per-component)

* [Snappy Command Line Utility](/../configuring_cluster/configuring_cluster#command-line)

* [Logging](/../configuring_cluster/configuring_cluster#logging)

* [Configuring SSH Login without Password](/../configuring_cluster/configuring_cluster.md#ssh)

* [SSL Setup for Client-Server](/../configuring_cluster/configuring_cluster.md#ssl)

* [SnappyData Properties](/../configuring_cluster/property_description.md)
