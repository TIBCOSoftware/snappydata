# Configuration Overview
SnappyData, a database server cluster, has three main components - Locator, Server, and Lead.

The Lead node embeds a Spark driver and the Server node embeds a Spark Executor. The server node also embeds a SnappyData store.

SnappyData cluster can be started with the default configurations using script `sbin/snappy-start-all.sh`. This script starts up a locator, one data server, and one lead node. However, SnappyData can be configured to start multiple components on different nodes. 
Also, each component can be configured individually using configuration files. In this document, 
we discuss how the components can be individually configured. We also discuss various other configurations of SnappyData.

The following topics are covered in this section:

* [Configuration Files](configuring_cluster/configuration_files.md)

* [HDFS with SnappyData Store](configuring_cluster/hdfs_with_snappydata_store.md)

* [Spark Specific Properties](configuring_cluster/spark_specific_properties.md)

* [Example for Multiple-Host Configuration](configuring_cluster/example_for_multiple-host.md)

* [Environment Settings](configuring_cluster/environment_settings.md)

* [Hadoop Provided Settings](configuring_cluster/hadoop_provided_settings.md)

* [Per Component Configuration](configuring_cluster/per_component_configuration.md)

* [Logging](configuring_cluster/logging.md)

* [Configuring SSH Login without Password](configuring_cluster/configuring_ssh_without_password.md)

* [Add Servers to the Cluster and Stop Servers](configuring_cluster/add_and_stop_servers.md)

* [Starting Members Individually using Command Line (without scripts)](configuring_cluster/starting_members_individually_cli.md)

