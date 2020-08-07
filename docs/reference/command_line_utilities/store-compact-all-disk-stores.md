# compact-all-disk-stores

Perform online compaction of TIBCO ComputeDB disk stores.

## Syntax

**For secured cluster**

```
./bin/snappy compact-all-disk-stores -locators==<addresses> -auth-provider=<authprovider> -user=<username> -password=<password> -gemfirexd.auth-ldap-search-base=<search-base-values> -gemfirexd.auth-ldap-search-dn=<search-dn-values> -gemfirexd.auth-ldap-search-pw=<password>
```

**For non-secured cluster**

```pre
./bin/snappy compact-all-disk-stores==
  <-locators=<addresses>> [-bind-address=<address>] [-<prop-name>=<prop-value>]*
```


The table describes options for `snappy compact-all-disk-stores`. 

|Option|Description|
|--------|--------|
|-locators|List of locators used to discover members of the distributed system. Supply all locators as comma-separated host:port values. The port is the `peer-discovery-port` used when starting the cluster (default 10334). This is a mandatory field.|
|-bind-address    |The address to which this peer binds for receiving peer-to-peer messages. By default `snappy` uses the hostname, or localhost if the hostname points to a local loopback address.|
|-prop-name</br> prop-value    |Any other TIBCO ComputeDB distributed system property.|
|Authentication properties| Refer [Authentication Properites](/security/launching_the_cluster_in_secure_mode.md#authproperties).|

## Description

When a CRUD operation is performed on a persistent/overflow table, the data is written to the log files. Any pre-existing operation record for the same row becomes obsolete, and TIBCO ComputeDB marks it as garbage. It compacts an old operation log by copying all non-garbage records into the current log and discarding the old files.

Manual compaction can be done for online and offline disk stores. For the online disk store, the current operation log is not available for compaction, no matter how much garbage it contains.

Offline compaction runs in the same way, but without the incoming CRUD operations. Also, because there is no current open log, the compaction creates a new one to get started.

## Online Compaction

To run manual online compaction, ALLOWFORCECOMPACTION should be set to true while [creating a diskstore](../sql_reference/create-diskstore.md)
You can run manual online compaction at any time while the system is running. Oplogs that are eligible for compaction, based on the COMPACTIONTHRESHOLD, are compacted into the current oplog.

## Example

**Secured cluster**

```
./bin/snappy compact-all-disk-stores -locators=locatorhostname:10334 -auth-provider=LDAP -user=snappy1 -password=snappy1  -J-Dgemfirexd.auth-ldap-server=ldap://localhost:389/ -J-Dgemfirexd.auth-ldap-search-base=cn=sales-group,ou=sales,dc=example,dc=com -J-Dgemfirexd.auth-ldap-search-dn=cn=admin,dc=example,dc=com -J-Dgemfirexd.auth-ldap-search-pw=user123

// The following output is displayed:

Connecting to distributed system: locators=localhost[10334]
18/11/15 17:54:02.964 IST main<tid=0x1> INFO SNAPPY: TraceAuthentication: Enabling authorization for auth provider LDAP
18/11/15 17:54:03.757 IST main<tid=0x1> INFO SnappyUnifiedMemoryManager: BootTimeMemoryManager org.apache.spark.memory.SnappyUnifiedMemoryManager@11a82d0f configuration:
		Total Usable Heap = 786.2 MB (824374722)
		Storage Pool = 393.1 MB (412187361)
		Execution Pool = 393.1 MB (412187361)
		Max Storage Pool Size = 628.9 MB (659499777)
Compaction complete.
The following disk stores compacted some files:

```

**Non-secured cluster**

```
./bin/snappy compact-all-disk-stores==locators=locatorhostname:10334*
```
