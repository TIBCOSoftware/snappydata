# unblock-disk-store
Indicates a member waiting for other diskStoreID to go ahead with the initialization. When a member recovers from a set of persistent files, it waits for other members that were also persisting the same region to start up. If the persistent files for those other members were lost or not available, this method can be used to tell the members to stop waiting for that data and consider its own data as latest.

## Syntax

```no-highlight
snappy unblock-disk-store <disk-store-id>
   <-locators=<addresses>> 
        [-bind-address=<address>] 
  [-<prop-name>=<prop-value>]*
```

The table describes options and arguments for snappy unblock-disk-store. If no multicast or locator options are specified on the command-line, the command uses the gemfirexd.properties file (if available) to determine the distributed system to which it should connect.

|Option|Description|
|-|-|
|disk-store-id|(Required.) Specifies the unique ID of the disk store to unblock.| 
|locators|List of locators used to discover members of the distributed system. Supply all locators as comma-separated host:port values. The port is the `peer-discovery-port` used when starting the cluster (default 10334). This is a mandatory field.|
|bind-address|The address to which this peer binds for receiving peer-to-peer messages. By default SnappyData uses the hostname, or localhost if the hostname points to a local loopback address.|
|prop-name|Any other SnappyData distributed system property.|


## Example

```no-highlight
snappy unblock-disk-store a395f237-c5e5-4e76-8024-353272e86f28 -locators=localhost:10334
Connecting to distributed system: -locators=localhost:10334
Unblock was successful and no disk stores are now waiting
```
