# revoke-missing-disk-store
Instruct SnappyData members to stop waiting for a disk store to become available.

## Syntax

```pre
./bin/snappy revoke-missing-disk-store <disk-store-id>
   <-locators=<addresses>> 
        [-bind-address=<address>] 
  [-<prop-name>=<prop-value>]*
```

The table describes options and arguments for snappy revoke-missing-disk-store. If no multicast or locator options are specified on the command-line, the command uses the gemfirexd.properties file (if available) to determine the distributed system to which it should connect.

|Option|Description|
|-|-|
|-disk-store-id|(Required.) Specifies the unique ID of the disk store to revoke.| 
|-locators|List of locators used to discover members of the distributed system. Supply all locators as comma-separated host:port values. The port is the `peer-discovery-port` used when starting the cluster (default 10334). This is a mandatory field.|
|-bind-address|The address to which this peer binds for receiving peer-to-peer messages. By default SnappyData uses the hostname, or localhost if the hostname points to a local loopback address.|
|-prop-name|Any other SnappyData distributed system property.|

<!--## Description


[Handling Missing Disk Stores](../../concepts/tables/persisting_table_data/handling_missing_disk_stores.md#handling_missing_disk_stores) provides more details about listing and revoking missing disk stores.
-->

## Example

This command first lists the missing disk store:

```pre
./bin/snappy list-missing-disk-stores -locators=localhost:10334
Connecting to distributed system: -locators=localhost:10334
1f811502-f126-4ce4-9839-9549335b734d [curwen.local:/Users/user1/snappydata/rowstore/SnappyData_RowStore_13_bNNNNN_platform/server2/./datadictionary]
```

Next, `snappy` revokes the missing disk store if more recent data is available:

```pre
./bin/snappy revoke-missing-disk-store 1f811502-f126-4ce4-9839-9549335b734d -locators=localhost:10334
Connecting to distributed system: -locators=localhost:10334
revocation was successful and no disk stores are now missing
```

Finally, `snappy` verifies that no disk stores are missing:

```pre
./bin/snappy list-missing-disk-stores -locators=localhost:10334
Connecting to distributed system: -locators=localhost:10334
The distributed system did not have any missing disk stores
```
