# SnappyData Properties

The cluster has three main components - Locator, Server, and Lead.

In this document, we provide the syntax and description of the properties you can set for each component. For information on configuring the cluster refer to [configuring the cluster](configuration.md).

The following topics are covered in this section:

* [Server](#server)

* [Locator](#locator)

* [Lead](#lead)

* [List of Properties](property_description.md)


<a id="server">

# Server Properties

A SnappyData server is the main server side component in a SnappyData system that provides connectivity to other servers, peers, and clients in the cluster. It can host data. A server is started using the *server* utility of the *SnappyData* launcher.

## Syntax

``` pre
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
                  [-member-timeout=<milliseconds>]
```

<a id="locator">
# Locator Properties

Allows peers (including other locators) in a distributed system to discover each other without the need to hard-code anything about other peers.

## Syntax

``` pre
snappy locator start [-J<vmarg>]* [-dir=<workingdir>] 
                  [-classpath=<classpath>]
                  [-heap-size=<size>][memory-size=<size>]
                  [-peer-discovery-address=<addr> (default is 0.0.0.0)]
                  [-peer-discovery-port=<port> (default 10334)]
                  [-bind-address=<address> (default is the -peer-discover-address value)]
                  [-client-bind-address=<addr> (default is
                     bind-address or if not set then loopback)]
                  [-client-port=<clientport> (default 1527)]
                  [-locators=<addresses>] 
                  [-log-file=<path> (default gfxdlocator.log)]
                  [-member-timeout=<milliseconds>]
```
<a id="lead">
# Lead Properties

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
                  [snappydata.column.batchsize=<number>]
```