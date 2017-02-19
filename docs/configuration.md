#Overview
SnappyData, a database server cluster, has three main components - Locator, Server, and Lead.

The Lead node embeds a Spark driver and the Server node embeds a Spark Executor. The server node also embeds a SnappyData store.

SnappyData cluster can be started with the default configurations using script `sbin/snappy-start-all.sh`. This script starts up a locator, one data server, and one lead node. However, SnappyData can be configured to start multiple components on different nodes. 
Also, each component can be configured individually using configuration files. In this document, 
we discuss how the components can be individually configured. We also discuss various other configurations of SnappyData.

