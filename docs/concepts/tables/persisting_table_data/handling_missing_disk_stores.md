# Handling Missing Disk Stores

Use the `snappy list-missing-disk-stores` command to find disk stores with the latest persisted data. Use `snappy revoke-missing-disk-stores` to stop waiting for the data when you cannot bring it online.

-   [Why Disk Stores Go Missing](#handling_missing_disk_stores__section_D727399DA07346FAB875C70A15B9CCA0)

-   [List Missing Disk Stores](#handling_missing_disk_stores__section_8A420CCC1D8A4EE984C2E1D1F4C57D59)

-   [Revoke Missing Disk Stores](#handling_missing_disk_stores__section_FDF161F935054AB190D9DB0D7930CEAA)

<a id="handling_missing_disk_stores__section_D727399DA07346FAB875C70A15B9CCA0"></a>

## Why Disk Stores Go Missing

Disk stores usually go missing because their member fails to start. The member can fail to start for a number of reasons, including:

-   Disk store file corruption. You can check on this by validating the disk store.

-   Incorrect distributed system configuration for the member

-   Network partitioning

-   Drive failure

!!! Note
	The disk store directories listed for missing disk stores may not be the directories you have currently configured for the member. The list is retrieved from the other running members—the ones who are reporting the missing member. They have information from the last time the missing disk store was online. If you move your files and change the member’s configuration, these directory locations will be stale. </p>

<a id="handling_missing_disk_stores__section_8A420CCC1D8A4EE984C2E1D1F4C57D59"></a>

## List Missing Disk Stores

The `snappy list-missing-disk-stores` command lists all disk stores with most recent data that are being waited on by other members.

For replicated tables, this command only lists missing members that are preventing other members from starting up. For partitioned tables, this command also lists any offline data stores, even when other data stores for the table are online, because their offline status may be causing `PartitionOfflineExceptions` in cache operations or preventing the system from satisfying redundancy.

Example:

``` pre
snappy list-missing-disk-stores
Connecting to distributed system: mcast=/239.192.81.2:12348
DiskStore at straw.gemstone.com /export/straw3/users/jpearson/testGemFire/hostB/DS1
```

!!!Note
	Make sure this `snappy` call can find a `gemfirexd.properties` file to locate the distributed system. Or, specify the multicast port or locator properties to connect to the cluster (for example, `-mcast-port=`*port_number*). </p>

<a id="handling_missing_disk_stores__section_FDF161F935054AB190D9DB0D7930CEAA"></a>

## Revoke Missing Disk Stores

This section applies to disk stores for which both of the following are true:

-   Disk stores that have the most recent copy of data for one or more tables or table buckets.

-   Disk stores that are unrecoverable, such as when you have deleted them, or their files are corrupted or on a disk that has had a catastrophic failure.

When you cannot bring the latest persisted copy online, use the revoke command to tell the other members to stop waiting for it. Once the store is revoked, the system finds the remaining most recent copy of data and uses that.

!!! Note
	Once revoked, a disk store cannot be reintroduced into the system. </p>

Use the `snappy list-missing-disk-stores` command to identify the disk store you need to revoke. The revoke command takes the host and directory in input, as listed by that command.

Example:

``` pre
snappy list-missing-disk-stores
Connecting to distributed system: mcast=/239.192.81.2:12348
DiskStore at straw.gemstone.com /export/straw3/users/jpearson/testGemFire/hostB/DS1
snappy revoke-missing-disk-store straw.gemstone.com /export/straw3/users/jpearson/testGemFire/hostB/DS1
Connecting to distributed system: mcast=/239.192.81.2:12348
revocation was successful ...
```

!!!Note 
	Make sure this `snappy` call can find a `gemfirexd.properties` file to locate the distributed system. Or, specify the multicast port or locator properties to connect to the cluster (for example, `-mcast-port=`*port_number*). </p>
