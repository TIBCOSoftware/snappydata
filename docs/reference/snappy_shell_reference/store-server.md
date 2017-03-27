# server

A SnappyData server is the main server side component in a SnappyData system that provides connectivity to other servers, peers, and clients in the cluster. It can host data. A server is started using the *server* utility of the *gfxd* launcher.

##Syntax

-   <a href="store-server.html#reference_13C28DC08CBF4F768EC8A9FFD90BD804__refsyn_EB4D6E14F1674E06BF7FA54098791D82" class="xref">Syntax</a>
-   <a href="store-server.html#reference_13C28DC08CBF4F768EC8A9FFD90BD804__section_C8742094214D47B5852D621AB083F825" class="xref">Description</a>
-   <a href="store-server.html#reference_13C28DC08CBF4F768EC8A9FFD90BD804__example_212607A340E341BC83EE30FB912A8A07" class="xref">Example: Servers and Client Using Locator</a>
-   <a href="store-server.html#reference_13C28DC08CBF4F768EC8A9FFD90BD804__example_3BD73A7BC1D6453E94D55027B88086AD" class="xref">Example: Multiple Locators for High Availability</a>
-   <a href="store-server.html#reference_13C28DC08CBF4F768EC8A9FFD90BD804__example_5B09B5C98F09461AA347A592CDD330A4" class="xref">Example: Servers and Accessors with a Locator</a>
-   <a href="store-server.html#reference_13C28DC08CBF4F768EC8A9FFD90BD804__example_EAD8AD13427C4144A1FD3B3D892E91BD" class="xref">Example: Servers and Locator Using BUILTIN Authentication</a>
-   <a href="store-server.html#reference_13C28DC08CBF4F768EC8A9FFD90BD804__example_8E7E7AB6CF19440593BDE8315691B502" class="xref">Example: Servers and Accessors with Server Groups and Locator</a>

To start a SnappyData server :

``` pre
snappy-shell rowstore server start [-J<vmarg>]* [-dir=<workingdir>] [-classpath=<classpath>]
                  [-sync=<false|true> (default false)]
                  [-heap-size=<size>] [-off-heap-size=<size>]
                  [-mcast-port=<port> (default 10334)]
                  [-mcast-address=<address> (default 239.192.81.1)]
                  [-locators=<addresses>] [-start-locator=<address>]
                  [-server-groups=<groups>] [-lock-memory]
                  [-rebalance] [-init-scripts=<sql-files>]
                  [-run-netserver=<true|false> (default true)]
                  [-bind-address=<address> (default is hostname or localhost 
                    if hostname points to a local loopback address)]
                  [-client-bind-address=<clientaddr> (default is localhost)]
                  [-client-port=<clientport> (default 1527)]
                  [-critical-heap-percentage=<percentage>
                    (default 90% if -heap-size is provided, otherwise 
                     not configured)]
                  [-eviction-heap-percentage=<eviction-heap-percentage>
                    (default 80% of critical-heap-percentage)]
                  [-critical-off-heap-percentage=<critical-off-heap-percentage>
                    (default 90% if -off-heap-size is provided, otherwise not
                     configured)]                     
                  [-eviction-off-heap-percentage=<eviction-off-heap-percentage>
                    (default 80% of critical-off-heap-percentage)]
                  [-host-data=<true|false> (default true)]
                  [-log-file=<path> (default gfxdserver.log)]
                  [-auth-provider=<provider>]
                  [-server-auth-provider=<provider>]
                  [-user=<username>] [-password[=<password>]]
                  [-<prop-name>=<prop-value>]*
```

!!! Caution:
	* Never move the working directory for a SnappyData server or locator while the member is running.
	* Never delete or modify SnappyData persistence files, or move the files from the member working directory.

To display the status of a running server:

``` pre
snappy-shell rowstore server status [ -dir=<workingdir> ]
```

To stop a running server:

``` pre
snappy-shell rowstore server stop [ -dir=<workingdir> ]
```

If you started a server from a script or batch file using the `-sync=false` option (the default), you can use the following command to wait until the server has finished synchronization and finished starting:

``` pre
snappy-shell rowstore server wait [-J<vmarg>]* [-dir=<workingdir>]
```

The `wait` command does not return control until the locator has completed startup.

To stop all running accessor and datastore members in the system, use the following command from within a server working directory:

``` pre
snappy-shell shut-down-all
```

The table describes the options of the snappy-shell rowstore server command. Default values are used if you do not specify an option.



|Option|Description|
|-|-|
|-J|JVM option passed to the spawned SnappyData server JVM. </br>For example, use -J-Xmx1024m to set the JVM heap to 1GB.|
|-dir|Working directory of the server that will contain the SnappyData Server status file and will be the default location for log file, persistent files, data dictionary, and so forth (defaults to the current directory) .|
|-classpath|Location of user classes required by the SnappyData Server.</br>This path is appended to the current classpath.|
|-heap-size|Sets the heap size for the Java VM, using SnappyData default resource manager settings. </br>For example, <code class="ph codeph">-heap-size=1024m</code>. </br>If you use the <code class="ph codeph">-heap-size</code> option, by default SnappyData sets the critical-heap-percentage to 90% of the heap size, and the eviction-heap-percentage to 80% of the 'critical-heap-percentage' (which when using defaults, corresponds to 72% of the heap size). </br>SnappyData also sets resource management properties for eviction and garbage collection if they are supported by the JVM. </br>**Note** The <code class="ph codeph">-initial-heap</code> and <code class="ph codeph">-max-heap</code> parameters, used in the earlier SQLFire product, are no longer supported. </br>Use <code class="ph codeph">-heap-size</code> instead.|
|-off-heap-size|Specifies the amount of off-heap memory to allocate on the server. </br>You can specify the amount in bytes (b), kilobytes (k), megabytes (m), or gigabytes (g). </br>For example, <code class="ph codeph">-off-heap-size=2g</code>.|
|-server-groups|Comma-separated list of server groups to which this member belongs. </br>Used for creating tables in particular sets of servers or for firing data-aware procedures in particular server groups. </br>See <a href="../language_ref/ref-create-table.html#create-table" class="xref" title="Creates a new table using SnappyData features.">CREATE TABLE</a> and <a href="../language_ref/ref-call-procedure.html#reference_47BF2460F6924D4F8CD81DA99BFAD783" class="xref" title="SnappyData extends the CALL statement to enable execution of Data-Aware Procedures (DAP)"> </a>. </br>These procedures can be routed to SnappyData members that host the required data.">CALL</a>. </br>If this option is not specified then the server only belongs to the `default` server group. </br>The default server group has no name and contains all members of the distributed system. </br> **Note**: SnappyData converts server group names to all-uppercase letters before storing the values in the SYS.MEMBERS table. </br>DDL statements and procedures automatically convert any supplied server group values to all-uppercase letters. </br>However, you must specify uppercase values for server groups when you directly query the SYS.MEMBERS table.|
|-lock-memory|Include this option to lock SnappyData heap and off-heap memory pages into RAM. </br>This prevents the operating system from swapping the pages out to disk, which can cause sever performance degradation. </br>As a best practice, Pivotal recommends that you set this option when you run SnappyData and Hadoop on the same machine.</br>**Note**: When you use this command, also configure the operating system limits for locked memory, as described in <a href="../../getting_started/topics/system_requirements.html#concept_system-requirements" class="xref" title="This topic describes the supported configurations and system requirements for SnappyData.">Supported Configurations and System Requirements</a>.|
|-run-netserver|If true, starts a network server that can service thin clients. </br>See the `-client-bind-address` and `-client-port` options to specify where the server should listen. </br>Defaults is true.</br>If set to false, the `-client-bind-address` and `-client-port` options have no effect.|
|-sync|Determines whether the <code class="ph codeph">snappy-shell rowstore server</code> command returns immediately if the server reaches a &quot;waiting&quot; state. </br>A locator or server reaches the &quot;waiting&quot; state on startup if the member depends on another server or locator to provide up-to-date disk store persistence files. </br>This type of dependency can occur if the locator or server that you are starting was not the last member to shut down in the distributed system, and another member stores the most recent version of persisted data for the system.</br>Specifying <code class="ph codeph">-sync=false</code> (the default) causes the <code class="ph codeph">gfxd</code> command to return control immediately after the member reaches &quot;waiting&quot; state. </br>With <code class="ph codeph">-sync=true</code>, the <code class="ph codeph">gfxd</code> command does not return control until after all dependent members have booted and the member has finished synchronizing disk stores.</br>Always use <code class="ph codeph">-sync=false</code> (the default) when starting multiple members on the same machine, especially when executing <code class="ph codeph">gfxd</code> commands from a shell script or batch file, so that the script file does not hang while waiting for a particular SnappyData member to start. </br>You can use the <code class="ph codeph">snappy-shell rowstore locator wait</code> and/or <code class="ph codeph">snappy-shell rowstore server wait</code> later in the script to verify that each server has finished synchronizing and has reached the "running"; state. </br>For example: </br>``` <pre class="pre codeblock"><code>#!/bin/bash` </br># Start all local SnappyData members to waiting state, regardless of which member holds the most recent </br># disk store files: </br>`snappy-shell rowstore locator start -dir=/locator1 -sync=false`</br>`snappy-shell rowstore server start -client-port=1528 -locators=localhost[10334] -dir=/server1 -sync=false`</br>`snappy-shell rowstore server start -client-port=1529 -locators=localhost[10334] -dir=/server2 -sync=false`</br># Wait until all members have finished synchronizing and starting:</br>`snappy-shell rowstore locator wait -dir=/locator1`</br>`snappy-shell rowstore server wait -dir=/server1`</br>`snappy-shell rowstore server wait -dir=/server2`</br># Continue any additional tasks that require access to the SnappyData members...```</br>[...]</code></pre></br>As an alternative to using <code class="ph codeph">snappy-shell rowstore server wait</code>, you can monitor the current status of SnappyData members using STATUS column in the <a href="../system_tables/members.html#reference_21873F7CB0454C4DBFDC7B4EDADB6E1F" class="xref noPageCitation" title="A SnappyData virtual table that contains information about each distributed system member.">MEMBERS</a> system table.|
|-rebalance|Causes the new member to trigger a rebalancing operation for all partitioned tables in the system. </br>The system always tries to satisfy the redundancy of all partitioned tables on new member startup regardless of this option.|
|-init-scripts|Specifies a comma-separated list of files containing the initial SQL commands to be executed in the same format as that required by the SnappyData command shell. </br>These commands are executed after completing the initial DDL replay from persisted data in this member or from existing members. </br>This brings the meta-data to consistent state with the cluster before executing the script.</br>In the script file, you can include commands that require the existence of tables and other objects. </br>For example, you might include DML statements that to load initial data into existing tables.|
|-client-address|The address to which this peer binds for receiving peer-to-peer messages. </br>By default gfxd uses the hostname, or localhost if the hostname points to a local loopback address.|
|-client-bind-address|Address to which the network controller binds for client connections. </br>This takes effect only if &quot;`-run-netserver`&quot; option is not set to false.|
|-client-port|Port that the network controller listens on for client connections, 1-65535 with default of 1527. </br>This takes effect only if &quot;`-run-netserver`&quot; option is not set to false.|
|-critical-heap-percentage|Sets the Resource Manager's critical heap threshold in percentage of the old generation heap, 0-100. </br>If you set <code class="ph codeph">-heap-size</code>, the default value for <code class="ph codeph">critical-heap-percentage</code> is set to 90% of the heap size. </br>Use this switch to override the default.</br>When this limit is breached, the system starts canceling memory-intensive queries, throws low memory exceptions for new SQL statements, and so forth, to avoid running out of memory.|
|-eviction-heap-percentage|Sets the memory usage percentage threshold (0-100) that the Resource Manager will use to start evicting data from the heap. </br>By default, the eviction threshold is 80% of whatever is set for <code class="ph codeph">-critical-heap-percentage</code>. </br>Use this switch to override the default.|
|-critical-off-heap-percentage|Sets the critical threshold for off-heap memory usage in percentage, 0-100. </br>When this limit is breached, the system starts canceling memory-intensive queries, throws low memory exceptions for new SQL statements, and so forth, to avoid running out of off-heap memory.|
|-eviction-off-heap-percentage|Sets the off-heap memory usage percentage threshold, 0-100, that the Resource Manager will use to start evicting data from off-heap memory. </br>By default, the eviction threshold is 80% of whatever is set for <code class="ph codeph">-critical-off-heap-percentage</code>. </br>Use this switch to override the default.|
|-locators|List of locators as comma-separated `host`:`port` values used to communicate with running locators in the system and thus discover other peers of the distributed system. </br>The list must include all locators in use, and must be configured consistently for every member of the distributed system.</br>The default is no locators, so the system uses multicast for peer discovery which is <strong>not</strong> supported in snappydata.</br>Use of locators is highly recommended for production systems. </br>|
|-start-locator|Specifies an embedded locator in this server that is started and stopped automatically with this server. </br>The locator is specified as &lt;address&gt;[&lt;port&gt;] (note the square brackets), or just &lt;port&gt;. </br>When address is not specified, the one specified in &quot;`-bind-address`&quot; is used, if set. </br>Otherwise the machine's default address is used.|
|-host-data|If set to false, this peer does not host table data and acts as an `accessor` member, still having the capability to route queries to appropriate `datastores` and aggregating the results.|
|-auth-provider|Authentication mechanism for client connections. </br>If `-server-auth-provider` is not specified, then this same mechanism is also used for joining the cluster. </br>Supported values are BUILTIN and LDAP. </br>Note that SnappyData enables SQL authorization by default if you specify a client authentication mechanism with <code class="ph codeph">-auth-provider</code>.</br>By default, no authentication is used.|
|-server-auth-provider|Authentication mechanism for joining the cluster and talking to other servers and locators in the cluster. </br>Supported values are BUILTIN and LDAP. </br>By default, SnappyData uses the value of `-auth-provider` if it is specified, otherwise no authentication is used.|
|-user|If the servers or locators have been configured to use authentication, this option specifies the user name to use for booting the server and joining the distributed system.|
|-password|If the servers or locators are configured for authentication, this option specifies the password for the user (specified with the -user option) to use for booting the server and joining the distributed system.</br>The password value is optional. </br>If you omit the password, `snappy-shell` prompts you to enter a password from the console.|
|-log-file|Path of the file to which this member writes log messages (default is <span class="ph filepath">gfxdserver.log</span> in the working directory)|
|-&lt;prop-name&gt;|Any other SnappyData boot property such as &quot;`log-level`&quot;. </br>For example, to start a SnappyData server as a JMX Manager, use the boot properties described in <a href="../../manage_guide/jmx/chapter_overview.html#setting_up_logging" class="xref" title="You can use the Java Management Extensions (JMX) with SnappyData for additional administrative and monitoring capability.">Using Java Management Extensions (JMX)</a>.</br>See <a href="../configuration/ConnectionAttributes.html#jdbc_connection_attributes" class="xref" title="You use JDBC connection properties, connection boot properties, and Java system properties to configure SnappyData members and connections.">Configuration Properties</a> for a complete list of boot properties.|



<a id="reference_13C28DC08CBF4F768EC8A9FFD90BD804__section_C8742094214D47B5852D621AB083F825"></a>
## Description

You can start servers that can host data (*data stores*) or those that do not host data (*accessors*) with the `snappy-shell` utility, but either kind of member can service all SQL statements by routing them to appropriate data stores and aggregating the results. Even for a member hosting data for a table, it is not necessary that all data be available in the same member, for example, for DMLs that reference partitioned tables (<a href="../language_ref/ref-create-table-clauses.html#topic_3F16D93EB7184D7C91C5650F22ECC1E1" class="xref noPageCitation" title="The SnappyData partitioning clause controls the locality and distribution of data in the given server groups. This is important for optimizing queries, and it is essential for cross-table joins. The clause can be one of column partitioning, range partitioning, list partitioning, or generic expression partitioning.">PARTITION BY Clause</a>). So routing to other stores may be required. In addition it is possible for a member to be a data store but still not host any data for a table due to no common <a href="../language_ref/ref-create-table-clauses.html#topic_54F95DD4AC764B2D932218BEC90ED61F" class="xref noPageCitation" title="Configure the SnappyData datastore members that host data for table.">SERVER GROUPS Clause</a>.

Starting a server generates output similar to the following (XXX is the path of current working directory):

``` pre
 Starting SnappyData Server using multicast for peer discovery: 239.192.81.1[10334]
Starting network server for SnappyData Server at address localhost/127.0.0.1[1527]
SnappyData Server pid: 2015 status: running
Logs generated in <XXX>/gfxdserver.log
```

This will start the server process locally and use the current working directory to store logs, statistics and the data dictionary, and use multicast for peer discovery (address 239.192.81.1, port 10334). Any persistent tables created using the default disk store will also use this directory to manage the data files.

The data dictionary is managed in a subdirectory called '*datadictionary*' and persisted by default. This subdirectory contains the persistent metadata of all the DDLs executed in the distributed system from any clients or peers, and is necessary for a SnappyData server or locator member to start up and function properly.

As the output above indicates, a *network server* is also started by default that binds to localhost on port 1527. This service allows thin clients to connect to the server and execute SQL commands using the <a href="http://en.wikipedia.org/wiki/DRDA" class="xref">DRDA protocol</a>.

A SnappyData server that you are starting may require other cluster members (locators or servers) to boot before it can confirm that its data is consistent with those members' data. Even with no persistent or overflow tables, each server locally persists a copy of the data dictionary and may remain in a "waiting" state until dependent locators or server come back online to ensure that it has all the latest updates to the data dictionary:

``` pre
Starting SnappyData Server using locators for peer discovery: localhost[10334]
Starting network server for SnappyData Server at address localhost/127.0.0.1[1528]
Logs generated in /Users/yozie/snappydata/rowstore/RowStore_13_b48393_Linux/server1/gfxdserver.log
RowStore Server pid: 9502 status: waiting
Region /_DDL_STMTS_META_REGION has potentially stale data. It is waiting for another member to recover the latest data.
My persistent id:

  DiskStore ID: ff7d62c5-4e03-4c74-975f-c8d3639c1cee
  Name: 
  Location: /10.0.1.31:/Users/yozie/snappydata/rowstore/RowStore_13_b48393_Linux/server1/./datadictionary

Members with potentially new data:
[
  DiskStore ID: ea249383-b103-43d5-957b-f9789eadd37c
  Name: 
  Location: /10.0.1.31:/Users/yozie/snappydata/rowstore/RowStore_13_b48393_Linux/server2/./datadictionary
]
Use the "snappy-shell list-missing-disk-stores" command to see all disk stores that are being waited on by other members. - See log file for details.
```

Use `-sync=false` when starting RowStore members in a shell script or batch file to return control to the script immediately after the member reaches the "waiting" state.

If another server is now started to join the cluster using the same multicast port (default port as for the first server above), the startup message shows the other members in the distributed system:

``` pre
 snappy-shell rowstore server start -dir=server2
-client-port=1528
Starting RowStore Server using multicast for
peer discovery: 239.192.81.1[10334]
Starting network server for RowStore Server at
address localhost/127.0.0.1[1528]
 Distributed system now has 2 members.
 Other members: serv1(2015:data store)<v0>:32013/48225
RowStore Server pid: 2032 status: running
Logs generated in <XXX>/gfxdserver.log
```

The italicized lines indicate the output regarding the IDs of other members in the distributed system.

The startup script maintains the running status of the server in a file <span class="ph filepath">.gfxdserver.ser</span>.

##Example: Multiple Servers Using Default Multicast Port for Peer Discovery

``` pre
-- start a server using default mcast-port (10334) for discovery,
-- with current directory as the working directory, and network server
–- running on default localhost:1527
snappy-shell rowstore server start
–- start a second server talking to the first with dir2 as working directory;
–- network server is started explicitly on localhost:1528
snappy-shell rowstore server start -dir=dir2 -client-port=1528
–- start another server talking to the first two with dir3 as working
–- directory; network server is disabled explicitly
snappy-shell rowstore server start -dir=dir3 -run-netserver=false
–- check from the RowStore command shell
snappy-shell
snappy> connect client 'localhost:1527';
snappy> select ID from sys.members;
–- output will show three members in the distributed system

–- stop everything
snappy> quit;
snappy-shell rowstore server stop -dir=dir3
snappy-shell rowstore server stop -dir=dir2
snappy-shell rowstore server stop
```

##Example: Servers and Client Using Locator

``` pre
–- start a locator for peer discovery on port 3241
-- listening on all addresses
snappy-shell rowstore locator start -peer-discovery-port=3241
–- start three servers as before using different client ports 
-- and using the above started locator
snappy-shell rowstore server start -dir=dir1 -locators=localhost:3241 
     -client-port=1528
snappy-shell rowstore server start -dir=dir2 -locators=localhost:3241 
     -client-port=1529
snappy-shell rowstore server start -dir=dir3 -locators=localhost:3241 
     -client-port=1530
–- check from the RowStore command shell
–- connect using the locator's client-port (default 1527) 
-- for load balanced connection to one of the servers 
-- transparently and for reliable failover in case the 
-- server goes down
snappy-shell
snappy> connect client 'localhost:1527';
snappy> select ID, KIND from sys.members;
–- output will show four members with three as data stores 
-- and one as locator

–- stop everything
snappy> quit;
snappy-shell rowstore server stop -dir=dir3
snappy-shell rowstore server stop -dir=dir2
snappy-shell rowstore server stop -dir=dir1
snappy-shell rowstore locator stop
```

##Example: Multiple Locators for High Availability

``` pre
–- start two locators that configured to talk 
-- to one another
snappy-shell rowstore locator start -peer-discovery-port=3241 
     -locators=localhost:3242
snappy-shell rowstore locator start -dir=loc2 -peer-discovery-port=3242 
     -locators=localhost:3241 -client-port=1528
–- start four servers that can talk to both 
-- the above locators
snappy-shell rowstore server start -dir=dir1 
     -locators=localhost:3241,localhost:3242 
     -client-port=1529
snappy-shell rowstore server start -dir=dir2 
     -locators=localhost:3241,localhost:3242 
     -client-port=1530
snappy-shell rowstore server start -dir=dir3 
     -locators=localhost:3241,localhost:3242 
     -client-port=1531
snappy-shell rowstore server start -dir=dir4 
     -locators=localhost:3241,localhost:3242 
     -client-port=1532
–- check all the members in the distributed system
snappy-shell
snappy> connect client 'localhost:1527';
snappy> select ID, KIND from sys.members order by KIND DESC;
-– output will show six members with two locators 
-- followed by four data stores
snappy> quit;

–- now bring down the first locator and check that 
-- new servers can still join
snappy-shell rowstore locator stop
snappy-shell rowstore server start -dir=dir5 
     -locators=localhost:3241,localhost:3242 
     -client-port=1533
–- check all the members in the distributed system again
snappy-shell
snappy> connect client 'localhost:1528';
snappy> select ID, KIND from sys.members order by KIND DESC;
–- output will show six members with one locator 
-- followed by five data stores

–- stop everything
snappy> quit;
snappy-shell rowstore server stop -dir=dir5
snappy-shell rowstore server stop -dir=dir4
snappy-shell rowstore server stop -dir=dir3
snappy-shell rowstore server stop -dir=dir2
snappy-shell rowstore server stop -dir=dir1
snappy-shell rowstore locator stop -dir=loc2
```

##Example: Servers and Accessors with a Locator

``` pre
–- start a locator for peer discovery on port 3241
snappy-shell rowstore locator start -peer-discovery-port=3241
–- start three servers using different client ports and 
-- using the above started locator
snappy-shell rowstore server start -dir=dir1 -locators=localhost:3241 -client-port=1528
snappy-shell rowstore server start -dir=dir2 -locators=localhost:3241 -client-port=1529
snappy-shell rowstore server start -dir=dir3 -locators=localhost:3241 -client-port=1530
–- start a couple of peers that will not host data
snappy-shell rowstore server start -dir=dir4 -locators=localhost:3241 -client-port=1531 -host-data=false
snappy-shell rowstore server start -dir=dir5 -locators=localhost:3241 -client-port=1532 -host-data=false
–- check from the RowStore command shell
–- connect using the locator's client-port (default 1527) 
-- for load balanced connection to one of the servers/accessors 
-- transparently and for reliable failover in case the 
-- server/accessor goes down
snappy-shell
snappy> connect client 'localhost:1527';
snappy> select ID, KIND from sys.members;
–- output will show six members with one as locator, three 
-- data stores and two accessors
```

##Example: Servers and Locator Using BUILTIN Authentication

``` pre
–- start a locator for peer discovery on port 3241 
-- with authentication; below specifies a builtin 
-- system user on the command-line itself
-- (snappydata.user.gem1=gem1) but it is recommended 
-- to be in gemfirexd.properties in encrypted form 
-- using the "snappy-shell encrypt-password" tool
snappy-shell rowstore locator start -peer-discovery-port=3241 -auth-provider=BUILTIN  -snappydata.user.gem1=gem1 -user=gem1 -password=gem1
–- start three servers using different client 
-- ports and using the above started locator
snappy-shell rowstore server start -dir=dir1 -locators=localhost:3241 -client-port=1528 -auth-provider=BUILTIN  -snappydata.user.gem1=gem1 -user=gem1 -password=gem1
snappy-shell rowstore server start -dir=dir2 -locators=localhost:3241 -client-port=1529 -auth-provider=BUILTIN  -snappydata.user.gem1=gem1 -user=gem1 -password=gem1
snappy-shell rowstore server start -dir=dir3 -locators=localhost:3241 -client-port=1530 -auth-provider=BUILTIN  -snappydata.user.gem1=gem1 -user=gem1 -password=gem1
–- check from the RowStore command shell
–- connect using the locator's client-port 
-- (default 1527) for load balanced
–- connection to one of the servers/accessors 
-- transparently and for reliable
-- failover in case the server/accessor goes down
snappy-shell
snappy> connect client 'localhost:1527;user=gem1;password=gem1';
–- add a new database user
snappy> call sys.create_user('snappydata.user.gem2', 'gem2');
–- check members
snappy> select ID, KIND from sys.members;
–- output will show four members with one as 
-- locator and three data stores
```

##Example: Servers and Accessors with Server Groups and Locator

``` pre
–- start a locator for peer discovery on port 3241
snappy-shell rowstore locator start -peer-discovery-port=3241

–- start three servers using different client 
-- ports and using the above started locator
-- using two server groups (ordersdb and 
-- customers) with one server in both
-- the groups

snappy-shell rowstore server start -dir=dir1 -locators=localhost:3241 -client-port=1528 -server-groups=ordersdb
snappy-shell rowstore server start -dir=dir2 -locators=localhost:3241 -client-port=1529 -server-groups=customers
snappy-shell rowstore server start -dir=dir3 -locators=localhost:3241 -client-port=1530 -server-groups=ordersdb,customers

–- start a couple of peers that will 
-- not host data but in both server 
-- groups; using server groups in 
-- accessors is only useful if 
-- executing data-aware procedures 
-- targeted to those members
snappy-shell rowstore server start -dir=dir4 -locators=localhost:3241 -client-port=1531 -host-data=false -server-groups=ordersdb,customers
snappy-shell rowstore server start -dir=dir5 -locators=localhost:3241 -client-port=1532 -host-data=false -server-groups=ordersdb,customers

–- check from the RowStore command shell
–- connect using the locator's client-port 
-- (default 1527) for load balanced
–- connection to one of the servers/accessors 
-- transparently and for reliable
-- failover in case the server/accessor 
-- goes down
snappy-shell
snappy> connect client 'localhost:1527';
snappy> select ID, KIND from sys.members;
–- example output
ID                             |KIND
-----------------------------------------------
pc29(23372)<v0>:28185/36245    |locator(normal) 

pc29(23742)<v3>:19653/33509    |datastore(normal)
pc29(23880)<v4>:37719/51114    |accessor(normal)

pc29(23611)<v2>:52031/53106    |datastore(normal)
pc29(24021)<v5>:58510/43678    |accessor(normal)

pc29(23503)<v1>:30307/36105    |datastore(normal)


6 rows selected


-- also check for server groups
snappy> select ID, KIND, SERVERGROUPS from sys.members;
-- example output
ID                             |KIND              |SERVERGROUPS
-----------------------------------------------------------------
pc29(23372)<v0>:28185/36245    |locator(normal) 
  |
pc29(23742)<v3>:19653/33509    |datastore(normal) |CUSTOMERS,ORDERSDB
pc29(23880)<v4>:37719/51114    |accessor(normal)
  |CUSTOMERS,ORDERSDB
pc29(23611)<v2>:52031/53106    |datastore(normal) |CUSTOMERS
pc29(24021)<v5>:58510/43678    |accessor(normal)
  |CUSTOMERS,ORDERSDB
pc29(23503)<v1>:30307/36105    |datastore(normal) |ORDERSDB


6 rows selected
```


