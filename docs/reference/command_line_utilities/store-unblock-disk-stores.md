# unblock-disk-store
Indicates a member waiting for other diskStoreID to go ahead with the initialization. When a member recovers from a set of persistent files, it waits for other members that were also persisting the same region to start up. If the persistent files for those other members were lost or not available, this method can be used to tell the members to stop waiting for that data and consider its own data as latest.

## Syntax

**Secured cluster**

```
./bin/snappy unblock-disk-store<disk-store-id> -locators=localhost:<addresses>  -auth-provider=<auth-provider> -user=<username> -password=<password> -gemfirexd.auth-ldap-server=ldap://<ldap-server-host>:<ldap-server-port>/ -gemfirexd.auth-ldap-search-base=<search-base-values> -gemfirexd.auth-ldap-search-dn=<search-dn-values> -gemfirexd.auth-ldap-search-pw=<password>
```

**Non-secured cluster**

```pre
./bin/snappy unblock-disk-store <disk-store-id>
   <-locators=<addresses>> 
        [-bind-address=<address>] 
  [-<prop-name>=<prop-value>]*
```

The table describes options and arguments for snappy unblock-disk-store. If no multicast or locator options are specified on the command-line, the command uses the gemfirexd.properties file (if available) to determine the distributed system to which it should connect.

|Option|Description|
|-|-|
|-disk-store-id|(Required.) Specifies the unique ID of the disk store to unblock.| 
|-locators|List of locators used to discover members of the distributed system. Supply all locators as comma-separated host:port values. The port is the `peer-discovery-port` used when starting the cluster (default 10334). This is a mandatory field.|
|-bind-address|The address to which this peer binds for receiving peer-to-peer messages. By default TIBCO ComputeDB uses the hostname, or localhost if the hostname points to a local loopback address.|
|-prop-name|Any other TIBCO ComputeDB distributed system property.|
|Authentication properties| Refer [Authentication Properites](/security/launching_the_cluster_in_secure_mode.md#authproperties).|


## Example

**Secured cluster**

```
./bin/snappy unblock-disk-store a395f237-c5e5-4e76-8024-353272e86f28 -locators=localhost:10334 -auth-provider=LDAP -gemfirexd.auth-ldap-server=ldap://<ldap-server-host>:389/ -user=<username> -password=<password> -gemfirexd.auth-ldap-search-base=<search-base-values> -gemfirexd.auth-ldap-search-dn=<search-dn-values> -gemfirexd.auth-ldap-search-pw=<password>

Connecting to distributed system: locators=localhost[10334]
18/11/16 16:26:56.050 IST main<tid=0x1> INFO SNAPPY: TraceAuthentication: Enabling authorization for auth provider LDAP
18/11/16 16:26:56.863 IST main<tid=0x1> INFO SnappyUnifiedMemoryManager: BootTimeMemoryManager org.apache.spark.memory.SnappyUnifiedMemoryManager@16943e88 configuration:
		Total Usable Heap = 786.2 MB (824374722)
		Storage Pool = 393.1 MB (412187361)
		Execution Pool = 393.1 MB (412187361)
		Max Storage Pool Size = 628.9 MB (659499777)
Unblock was successful and no disk stores are now waiting
```

**Non-secured cluster**

```pre
./bin/snappy unblock-disk-store a395f237-c5e5-4e76-8024-353272e86f28 -locators=localhost:10334
Connecting to distributed system: -locators=localhost:10334
Unblock was successful and no disk stores are now waiting
```
