#locator

Allows peers (including other locators) in a distributed system to discover each other without the need to hard-code anything about other peers.

##Syntax

To start a stand-alone locator use the command:

``` pre
snappy locator start [-J<vmarg>]* [-dir=<workingdir>] 
                   [-classpath=<classpath>]
                   [-distributed-system-id=<id>]
                   [-heap-size=<size>]
                   [-peer-discovery-address=<addr> (default is 0.0.0.0)]
                   [-peer-discovery-port=<port> (default 10334)]
                   [-sync=<true|false> (default false)]
                   [-bind-address=<address> (default is the -peer-discover-address value)]
                   [-run-netserver=<true|false> (default true)]
                   [-client-bind-address=<addr> (default is
                     bind-address or if not set then loopback)]
                   [-client-port=<clientport> (default 1527)]
                   [-locators=<addresses>] 
                   [-log-file=<path> (default gfxdlocator.log)]
                   [-remote-locators=<host[port]>[,<host[port]>]...]
                   [-auth-provider=<provider>]
                   [-server-auth-provider=<provider>]
                   [-user=<username>] [-password[=<password>]]
                   [-<prop-name>=<prop-value>]*
```

!!! Note:
	When starting a locator using `snappy locator start`, in addition to using the options listed above you can use any other SnappyData boot properties (`-<prop-name>=<prop-value>`) on the command-line.

!!! CAUTION:
	Never move the working directory for a SnappyData server or locator while the member is running.

!!! CAUTION:
	Never delete or modify SnappyData persistence files, or move the files from the member working directory.

The startup script maintains the running status of the locator in a file <span class="ph filepath">.gfxdlocator.ser</span>.

To display the status of a running locator:

``` pre
snappy locator status [ -dir=<workingdir> ]
```

To stop a running locator:

``` pre
snappy locator stop [ -dir=<workingdir> ]
```

If you started a locator from a script or batch file using the `-sync=false` option (the default), you can use the following command to wait until the locator has finished synchronization and finished starting:

``` pre
snappy locator wait [-J<vmarg>]* [-dir=<workingdir>]
```

The `wait` command does not return control until the locator has completed startup.

The table below describes options for all of the above `snappy locator` commands. Default values are used if you do not specify an option.


|Option||Description|
|-|-|
|-J|JVM option passed to the spawned SnappyData Locator JVM. For example, use -J-Xmx1024m to set the JVM heap to 1GB.|
|-dir|Working directory of the locator that will contain the SnappyData Locator status file and will be the default location for log file, persistent files, data dictionary, and so forth. This option defaults to the current directory.|
|-classpath|Location of user classes required by the SnappyData Locator. This path is appended to the current classpath.|
|-distributed-system-id|Integer that uniquely identifies this SnappyData cluster.</br>Set a unique value when using WAN replication between multiple SnappyData clusters. <mark>[Configure Locators for WAN Member Discovery](http://rowstore.docs.snappydata.io/docs/config_guide/topics/gateway-hubs/wan-locators.html) provides more information.TO BE CONFIRMED (ROWSTORE LINK)</mark>|
|-heap-size|Set a fixed heap size and for the Java VM, using SnappyData default resource manager settings. If you use the `-heap-size` option, by default SnappyData sets the critical-heap-percentage to 80% of the heap, and the eviction-heap-percentage to 80% of the critical heap. SnappyData also sets resource management properties for eviction and garbage collection if they are supported by the JVM. </br>**Note**: The `-initial-heap` and `-max-heap` parameters, used in the earlier SQLFire product, are no longer supported. Use `-heap-size` instead.|
|-peer-discovery-address|Address to which the locator binds for peer discovery (includes servers as well as other locators).|
|-peer-discovery-port|Port on which the locator listens for peer discovery (includes servers as well as other locators). </br>Valid values are in the range 1-65535, with a default of 10334.|
|-sync|Determines whether the `snappy locator` command returns immediately if the locator reaches a &quot;waiting&quot; state. A locator or server reaches the "waiting"; state on startup if the member depends on another server or locator to provide up-to-date disk store persistence files. This type of dependency can occur if the locator or server that you are starting was not the last member to shut down in the distributed system, and another member stores the most recent version of persisted data for the system. </br>Specifying `-sync=false` (the default) causes the `gfxd` command to return control immediately after the member reaches &quot;waiting&quot; state. With `-sync=true` (the default for servers), the `gfxd` command does not return control until after all dependent members have booted and the member has finished synchronizing disk stores. </br>Always use `-sync=false` (the default) when starting multiple members on the same machine, especially when executing `gfxd`commands from a shell script or batch file, so that the script file does not hang while waiting for a particular SnappyData member to start. You can use the `snappy locator wait` and/or `gfxd server wait`later in the script to verify that each server has finished synchronizing and has reached the "running"; state. </br>For example: </br> ```#!/bin/bash </br> # Start all local SnappyData members to waiting state, regardless of which member holds the most recent </br># disk store files: </br>snappy locator start -dir=/locator1 -sync=false </br> snappy server start -client-port=1528 -locators=localhost[10334] -dir=/server1 -sync=false </br>snappy server start -client-port=1529 -locators=localhost[10334] -dir=/server2 -sync=false </br> # Wait until all members have finished synchronizing and starting:</br> snappy locator wait -dir=/locator1</br>snappy server wait -dir=/server1</br>snappy server wait -dir=/server2</br># Continue any additional tasks that require access to the SnappyData members...[...]``` </br>As an alternative to using `snappy locator wait`, you can monitor the current status of SnappyData members using STATUS column in the [MEMBERS](../../reference/system_tables/members.md) system table.|
|-bind-address|The address to which this peer binds for receiving peer-to-peer messages. By default gfxd uses the -peer-discovery-address value.|
|-run-netserver|</br>If true then it starts a network server (see the `-client-bind-address` and `-client-port` options to specify where the server should listen) that can service thin clients (defaults to true).</br>If set to false, then the `-client-bind-address` and `-client-port` options have no affect. This option defaults to true.|
|-client-bind-address|</br>Address to which the network controller binds for client connections. This takes effect only if `-run-netserver` option is not set to false.|
|-client-port|</br>Port that the network controller listens on for client connections, 1-65535 with default of 1527. This takes effect only if `-run-netserver` option is not set to false.|
|-locators|</br>List of other locators as comma-separated host:port values used as backups in case one of the locators fails. The list must include all other locators in use, and must be configured consistently for every member of the distributed system.</br>Servers must be configured to use all the locators of the distributed system, which includes this locator and all of the others in the list.|
|-remote-locators|</br>Comma-separated list of addresses and port numbers of locators for remote SnappyData clusters.</br>Use this option when configuring WAN replication to identify connections to remote SnappyData clusters. <mark> TO BE CONFIRMED (RowStore link) [Configure Locators for WAN Member Discovery](http://rowstore.docs.snappydata.io/docs/config_guide/topics/gateway-hubs/wan-locators.html#concept_40733264812F4EB0B2E940AB4CC5FA9B) </mark> provides more information.|
|-auth-provider|Sets the authentication mechanism to use for client connections. Valid values are BUILTIN and LDAP. If you omit `-server-auth-provider`, then this same mechanism is also used for joining the cluster. If you omit both options, then no authentication mechanism is used. See <mark> [Configuring Security](http://rowstore.docs.snappydata.io/docs/deploy_guide/Topics/security/security_chapter.html#concept_6CD7D8A0C41E4BD2B0A5A8A6B192537F). TO BE CONFIRMED (RowStore link) </mark>Note that SnappyData enables SQL authorization by default if you specify a client authentication mechanism with `-auth-provider`.|
|-server-auth-provider|</br>The authentication mechanism to use for joining the cluster and talking to other servers and locators in the cluster. Supported values are BUILTIN and LDAP. By default, SnappyData uses the value of `-auth-provider` if it is specified, otherwise no authentication is used.|
|-user||</br>If the servers or locators have been configured to use authentication, this option specifies the user name to use for booting the server and joining the distributed system.|
|-password|</br>If the servers or locators have been configured to use authentication, this option specifies the password for the user (specified with the `-user` option) to use for booting the server and joining the distributed system.</br>The password value is optional. If you omit the password, `snappy` prompts you to enter a password from the console.|
|-log-file|</br>Path of the file to which this locator writes log messages. The default is <span class="ph filepath">gfxdlocator.log</span> in the working directory.|
|-&lt;prop-name&gt;|</br>Any other SnappyData boot property such as `log-level`. For example, to start a SnappyData locator as a JMX Manager, use the boot properties described in <mark> [Using Java Management Extensions (JMX)](http://rowstore.docs.snappydata.io/docs/manage_guide/jmx/chapter_overview.html#setting_up_logging). To be Confirmed RowStore link</mark>|</br>See [Configuration Properties](../../reference/configuration_parameters/config_parameters.md) for a complete list of boot properties.

##Description

A SnappyData locator allows peers (including other locators) in a distributed system to discover one another without having to hard-code anything about other peers. The other mode of discovery is using multicast that requires the peers to use the same multicast address and port for discovery.

A locator can be started on its own, or can be embedded in a server with the full SnappyData engine. To start embedded locators use the "*-start-locator*" option to the SnappyData server.

!!!Note: 
	Locators are the recommended discovery method for production systems. Running standalone locators provides the highest reliability and availability for the locator service as a whole. 

Because a locator has no data or meta-data available, it cannot directly service any of the user table related SQL statements from thin clients. It can only service queries involving inbuilt system tables, and inbuilt system procedure calls. Clients use the connection as a *control connection* to query the locator for the server with the least amount of load (measured in terms of other active client connections) and transparently create the actual *data connection* to the server as returned by the locator. The client also keeps track of all the other locators and peers in the distributed system by querying the locator. This practice avoids creating multiple control connections for the same distributed system. If the client detects any two separate user connections referring to locators or servers in the same distributed system then it will use the same *control connection* for both. The list of all locators and peers is also used for failover if the control connection itself fails.

SnappyData locators are necessary for load-balanced client connections, and for full availability of the transparent failover functionality provided by the thin client driver (high-availability, or HA in short). Without locators client connections cannot be load-balanced. Even though clients try to fail over transparently to other servers without locators, this behavior is not as reliable as using locators, and failover may not work transparently if multiple servers fail in quick succession.

Always start locator members before starting data stores when you boot a SnappyData distributed system. Converseley, always shut down locators last, after shutting down data store members (preferably with <mark> TO BE CONFIRMED RowStore link [shut-down-all](http://rowstore.docs.snappydata.io/docs/reference/store_commands/store-shut-down-all.html#reference_FF886BB14E5949B79E47AC334D23EEE5)) </mark>. Each locator locally persists a copy of the data dictionary required, and may have to wait for other locators to come back online to ensure that it has all the latest updates to the data dictionary.

##Example

Starting a locator generates output such as the following. XXX is the path of current working directory.

``` pre
Starting SnappyData Locator using peer discovery on: 0.0.0.0[10334]
Starting network server for SnappyData Locator at address localhost/127.0.0.1[1527] 
SnappyData Locator pid: 3722 status:running
Logs generated in <XXX>/gfxdlocator.log
```

By default `snappy locator start` starts the server process locally and uses the current working directory to store logs, locator state and the data dictionary, and uses default TCP port 10334 for peer discovery.

A sub-directory called <span class="ph filepath">datadictionary</span> is created by default in the current working directory of the locator and contains the persistent meta-data of the DDLs required for startup by locator (for example, authentication related privileges) that have been executed in the distributed system from any of the clients or peers. This directory is necessary for a SnappyData locator to startup and function properly.

As the output above indicates, a *network server* is also started by default that binds to localhost on port 1527. This service allows thin clients to connect to one of the servers and execute SQL commands using the [DRDA protocol](http://en.wikipedia.org/wiki/DRDA).

If you are restarting a locator that was used in a distributed system, the locator may need to wait for another member (server or locator) to join the system in order to synchronize disk store persistence files. In this case, the locator reaches a "waiting" state and does not complete startup until the dependent members join the system. For example:
<mark> Reference to rowstore To be confirmed </mark>

``` pre
Starting SnappyData Locator using peer discovery on: 0.0.0.0[10334]
Starting network server for SnappyData Locator at address localhost/127.0.0.1[1527]
Logs generated in /Users/yozie/snappydata/rowstore/RowStore_13_b48393_Linux/locator1/gfxdlocator.log
RowStore Locator pid: 9496 status: waiting
Region /_DDL_STMTS_META_REGION has potentially stale data. It is waiting for another member to recover the latest data.
My persistent id:

  DiskStore ID: ff28402d-4fa1-488f-ba47-7bf9dec8248e
  Name: 
  Location: /10.0.1.31:/Users/yozie/snappydata/rowstore/RowStore_13_b48393_Linux/locator1/./datadictionary

Members with potentially new data:
[
  DiskStore ID: ea249383-b103-43d5-957b-f9789eadd37c
  Name: 
  Location: /10.0.1.31:/Users/yozie/snappydata/rowstore/RowStore_13_b48393_Linux/server2/./datadictionary
, 
  DiskStore ID: ff7d62c5-4e03-4c74-975f-c8d3639c1cee
  Name: 
  Location: /10.0.1.31:/Users/yozie/snappydata/rowstore/RowStore_13_b48393_Linux/server1/./datadictionary
]
Use the "snappy list-missing-disk-stores" command to see all disk stores that are being waited on by other members. - See log file for details.
```

The locator synchronizes persistent data and completes startup after the dependent members join the system.
