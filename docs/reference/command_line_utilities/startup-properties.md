# Server
A SnappyData server is the main server side component in a SnappyData system that provides connectivity to other servers, peers, and clients in the cluster. It can host data. A server is started using the *server* utility of the *SnappyData* launcher.

## Syntax
``` pre
snappy server start [-J<vmarg>]* [-dir=<workingdir>] [-classpath=<classpath>]
                  
snappy server start [-J<vmarg>]* [-dir=<workingdir>] [-classpath=<classpath>]
                  [-classpath=<classpath>]
                  [-heap-size=<size>] [-memory-size=<size>]
                  [-locators=<addresses>] 
                  [-rebalance] [-init-scripts=<sql-files>]
                  [-bind-address=<address> (default is hostname or localhost 
                    if hostname points to a local loopback address)]
                  [-client-bind-address=<clientaddr> (default is localhost)]
                  [-client-port=<clientport> (default 1527)]
                  [-critical-heap-percentage=<percentage>
                    (default 90% if -heap-size is provided, otherwise 
                     not configured)]
                  [-eviction-heap-percentage=<eviction-heap-percentage>
                    (default 81% of critical-heap-percentage)]
                  [-log-file=<path> (default gfxdserver.log)]
                  [-J-Dgemfirexd.hostname-for-clients]=<IP or Host name>
```

# Locator

Allows peers (including other locators) in a distributed system to discover each other without the need to hard-code anything about other peers.

## Syntax

To start a stand-alone locator use the command:

``` pre
snappy locator start [-J<vmarg>]* [-dir=<workingdir>] 
                  [-classpath=<classpath>]
                  [-heap-size=<size>]
                  [-peer-discovery-address=<addr> (default is 0.0.0.0)]
                  [-peer-discovery-port=<port> (default 10334)]
                  [-bind-address=<address> (default is the -peer-discover-address value)]
                  [-client-bind-address=<addr> (default is
                     bind-address or if not set then loopback)]
                  [-client-port=<clientport> (default 1527)]
                  [-locators=<addresses>] 
                  [-log-file=<path> (default gfxdlocator.log)]
```

# Lead

## Syntax

```pre
leader start [-J<vmarg>]* [-dir=<workingdir>] [-classpath=<classpath>]
                  [-heap-size=<size>] [memory-size=<size>]
                  [-locators=<addresses>] 
                  [-rebalance] [-init-scripts=<sql-files>]
                  [-bind-address=<addr> (default is hostname or localhost
                        if hostname points to a local loopback address)]
                  [-critical-heap-percentage=<critical-heap-percentage>
                    (default 90% if -heap-size is provided, otherwise not 
                    configured)]
                  [-eviction-heap-percentage=<eviction-heap-percentage>
                    (default 80% of critical-heap-percentage)]
                  [-critical-off-heap-percentage=<critical-off-heap-percentage>
                    (default 90% if -off-heap-size is provided, otherwise not
                    configured)]
                  [-eviction-off-heap-percentage=<eviction-off-heap-percentage>
                    (default 80% of critical-off-heap-percentage)]
                  [-log-file=<path> (default gfxdserver.log)]
```

## Option Description
|Option|Description|
|-|-|
|-J|JVM option passed to the spawned SnappyData server JVM. </br>For example, use -J-Xmx1024m to set the JVM heap to 1GB.|
|-dir|Working directory of the server that will contain the SnappyData Server status file and will be the default location for log file, persistent files, data dictionary, and so forth (defaults to the current directory.|
|[-classpath|Location of user classes required by the SnappyData Server.</br>This path is appended to the current classpath.|
|-heap-size|Sets the heap size for the Java VM, using SnappyData default resource manager settings. </br>For example, -heap-size=1024m. </br>If you use the `-heap-size` option, by default SnappyData sets the critical-heap-percentage to 90% of the heap size, and the `eviction-heap-percentage` to 81% of the `critical-heap-percentage`. </br>SnappyData also sets resource management properties for eviction and garbage collection if they are supported by the JVM. |
|-locators|List of locators as comma-separated host:port values used to communicate with running locators in the system and thus discover other peers of the distributed system. </br>The list must include all locators in use, and must be configured consistently for every member of the distributed system.</br>The default is no locators, so the system uses multicast for peer discovery which is not supported in SnappyData. </br>Use of locators is highly recommended for production systems. |
|-rebalance|Causes the new member to trigger a rebalancing operation for all partitioned tables in the system. </br>The system always tries to satisfy the redundancy of all partitioned tables on new member startup regardless of this option.|
|-bind-address||
|-critical-heap-percentage|Sets the Resource Manager's critical heap threshold in percentage of the old generation heap, 0-100. </br>If you set `-heap-size`, the default value for `critical-heap-percentage` is set to 90% of the heap size. </br>Use this switch to override the default.</br>When this limit is breached, the system starts canceling memory-intensive queries, throws low memory exceptions for new SQL statements, and so forth, to avoid running out of memory.|
|-eviction-heap-percentage|Sets the memory usage percentage threshold (0-100) that the Resource Manager will use to start evicting data from the heap. By default, the eviction threshold is 81% of whatever is set for `-critical-heap-percentage`.</br>Use this switch to override the default.|
|-critical-off-heap-percentage|Sets the critical threshold for off-heap memory usage in percentage, 0-100. </br>When this limit is breached, the system starts canceling memory-intensive queries, throws low memory exceptions for new SQL statements, and so forth, to avoid running out of off-heap memory.|
|-eviction-off-heap-percentage|Sets the off-heap memory usage percentage threshold, 0-100, that the Resource Manager uses to start evicting data from off-heap memory. </br>By default, the eviction threshold is 81% of whatever is set for `-critical-off-heap-percentage`. </br>Use this switch to override the default.|
|-log-file|Path of the file to which this member writes log messages (default is gfxdserver.log in the working directory)|
|-J-Dgemfirexd.hostname-for-clients|IP address of the locator or server.|
|-peer-discovery-address|Address to which the locator binds for peer discovery (includes servers as well as other locators).|
|-peer-discovery-port|Port on which the locator listens for peer discovery (includes servers as well as other locators).  </br>Valid values are in the range 1-65535, with a default of 10334.|
