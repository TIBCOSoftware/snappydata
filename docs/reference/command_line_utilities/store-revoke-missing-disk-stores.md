# revoke-missing-disk-store
Instruct TIBCO ComputeDB members to stop waiting for a disk store to become available.

## Syntax

**Secured cluster**

```pre
./bin/snappy revoke-missing-disk-store -locators=<addresses> -auth-provider=<auth-provider> -user=<username> -password=<password> -gemfirexd.auth-ldap-server=ldap://<ldap-server-host>:<ldap-server-port>/ -gemfirexd.auth-ldap-search-base=<search-base-values> -gemfirexd.auth-ldap-search-dn=<search-dn-values> -gemfirexd.auth-ldap-search-pw=<password>

```
**Non-secured cluster**

```pre
./bin/snappy revoke-missing-disk-store <disk-store-id>
   <-locators=<addresses>> 
        [-bind-address=<address>] 
  [-<prop-name>=<prop-value>]*
```

The table describes options and arguments for snappy `revoke-missing-disk-store`. If no multicast or locator options are specified on the command-line, the command uses the **gemfirexd.properties** file (if available) to determine the distributed system to which it should connect.

|Option|Description|
|-|-|
|-disk-store-id|(Required.) Specifies the unique ID of the disk store to revoke.| 
|-locators|List of locators used to discover members of the distributed system. Supply all locators as comma-separated host:port values. The port is the `peer-discovery-port` used when starting the cluster (default 10334). This is a mandatory field.|
|-bind-address|The address to which this peer binds for receiving peer-to-peer messages. By default SnappyData uses the hostname, or localhost if the hostname points to a local loopback address.|
|-prop-name|Any other TIBCO ComputeDB distributed system property.|
|Authentication properties| Refer [Authentication Properites](/security/launching_the_cluster_in_secure_mode.md#authproperties).|


<!--## Description


[Handling Missing Disk Stores](../../concepts/tables/persisting_table_data/handling_missing_disk_stores.md#handling_missing_disk_stores) provides more details about listing and revoking missing disk stores.
-->

## Example

**Secured cluster**

The following example depicts how to revoke the missing disk stores in a secured cluster:

Using the following command, you must first list the missing disk stores:

```pre
./bin/snappy list-missing-disk-stores -locators=localhost:10334 -auth-provider=LDAP -user=snappy1 -password=snappy1  -J-Dgemfirexd.auth-ldap-server=ldap://localhost:389/ -J-Dgemfirexd.auth-ldap-search-base=cn=sales-group,ou=sales,dc=example,dc=com -J-Dgemfirexd.auth-ldap-search-dn=cn=admin,dc=example,dc=com -J-Dgemfirexd.auth-ldap-search-pw=user123
```
Next, run the `revoke-missing-disk-store` command to revoke the missing disk stores in case more recent data is available:

```pre
./bin/snappy revoke-missing-disk-store -locators=localhost:10334 -auth-provider=LDAP -gemfirexd.auth-ldap-server=ldap://<ldap-server-host>:389/ -user=<username> -password=<password> -gemfirexd.auth-ldap-search-base=<search-base-values> -gemfirexd.auth-ldap-search-dn=<search-dn-values> -gemfirexd.auth-ldap-search-pw=<password>

Connecting to distributed system: locators=localhost[10334]
18/11/16 16:24:37.187 IST main<tid=0x1> INFO SNAPPY: TraceAuthentication: Enabling authorization for auth provider LDAP
18/11/16 16:24:38.025 IST main<tid=0x1> INFO SnappyUnifiedMemoryManager: BootTimeMemoryManager org.apache.spark.memory.SnappyUnifiedMemoryManager@16943e88 configuration:
		Total Usable Heap = 786.2 MB (824374722)
		Storage Pool = 393.1 MB (412187361)
		Execution Pool = 393.1 MB (412187361)
		Max Storage Pool Size = 628.9 MB (659499777)
revocation was successful and no disk stores are now missing

```
Finally, you can use the same `list-missing-disk-stores `command to confirm that no disk stores are missing.

**Non-secured cluster**

The following example depicts how to revoke the missing disk stores in a non-secured cluster:

Using the following command, you must first list the missing disk stores:

```pre
./bin/snappy list-missing-disk-stores -locators=localhost:10334

Connecting to distributed system: -locators=localhost:10334
1f811502-f126-4ce4-9839-9549335b734d [curwen.local:/Users/user1/snappydata/rowstore/SnappyData_RowStore_13_bNNNNN_platform/server2/./datadictionary]
```
 
Next, run the `revoke-missing-disk-store` command to revoke the missing disk stores in case more recent data is available:

```pre
./bin/snappy revoke-missing-disk-store 1f811502-f126-4ce4-9839-9549335b734d -locators=localhost:10334

Connecting to distributed system: -locators=localhost:10334
revocation was successful and no disk stores are now missing
```
Finally, use the `list-missing-disk-stores` command to confirm that none of the disk stores are missing.
