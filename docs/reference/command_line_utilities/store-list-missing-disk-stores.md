# list-missing-disk-stores

Lists all disk stores with the most recent data for which other members are waiting.

##Syntax

``` pre
snappy list-missing-disk-stores 
  [-locators=<addresses>] [-bind-address=<address>] [-<prop-name>=<prop-value>]*
```

If no locator option is specified on the command-line, the command uses the gemfirexd.properties file (if available) to determine the distributed system to which it should connect.

The table describes options for snappy list-missing-disk-stores.

|Option|Description|
|-|-|
|-locators|List of locators used to discover members of the distributed system. Supply all locators as comma-separated host:port values.|
|-bind-address|The address to which this peer binds for receiving peer-to-peer messages. By default gfxd uses the hostname, or localhost if the hostname points to a local loopback address.|
|-prop-name|Any other SnappyData distributed system property.|

<!--
##Description

[Handling Missing Disk Stores](../../concepts/tables/persisting_table_data/handling_missing_disk_stores.md#handling_missing_disk_stores) provides more details about listing and revoking missing disk stores.
-->

##Example

``` pre
snappy list-missing-disk-stores 
Connecting to distributed system: -locators=localhost:10334
1f811502-f126-4ce4-9839-9549335b734d [curwen.local:/Users/user1/snappydata/rowstore/SnappyData_RowStore_13_bNNNNN_platform/server2/./datadictionary]
```
