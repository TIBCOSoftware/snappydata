# Overview
SnappyData, a database server cluster, has three main components - Locator, Server, and Lead.

The Lead node embeds a Spark driver and the Server node embeds a Spark Executor. The server node also embeds a SnappyData store.

SnappyData cluster can be started with the default configurations using script `sbin/snappy-start-all.sh`. This script starts up a locator, one data server, and one lead node. However, SnappyData can be configured to start multiple components on different nodes. 
Also, each component can be configured individually using configuration files. In this document, 
we discuss how the components can be individually configured. We also discuss various other configurations of SnappyData.

The following topics are covered in this section:

* [Configuration Files](/../configuration.md#configuration-files)

* [HDFS with SnappyData Store](/../configuration.md#hdfs-with-snappydata-store)

* [SnappyData Properties](/../snappydata_properties/property_description/)

<a id="configuration-files"></a>
<a id="configuring-leads"></a>
<a id="configuring-data-servers"></a>
<a id="snappydata-specific-properties"></a>
<a id="example-for-multiple-host-configuration"></a>
<a id="environment-settings"></a>
<a id="hadoop-provided-settings"></a>
<a id="per-component-configuration"></a>
<a id="snappy-command-line-utility"></a>
<a id="logging"></a>
<a id="configuring-ssh-login-without-password"></a>
<a id="ssl-setup-for-client-server"></a>