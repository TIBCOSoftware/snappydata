# list-missing-disk-stores

Lists all disk stores with the most recent data for which other members are waiting.

##Syntax

``` pre
snappy-shell list-missing-disk-stores [-mcast-port=<port>]
  [-mcast-address=<address>]
  [-locators=<addresses>] [-bind-address=<address>] [-<prop-name>=<prop-value>]*
```

If no multicast or locator options are specified on the command-line, the command uses the <span class="ph filepath">gemfirexd.properties</span> file (if available) to determine the distributed system to which it should connect.

The table describes options for snappy-shell list-missing-disk-stores.

|Option|Description|
|-|-|
|-mcast-port|Multicast port used to communicate with other members of the distributed system. If zero, multicast is not used for member discovery (specify `-locators` instead).</br>Valid values are in the range 0–65535, with a default value of 10334.|
|-mcast-address|Multicast address used to discover other members of the distributed system. This value is used only if the `-locators` option is not specified.</br>The default multicast address is 239.192.81.1.|
|-locators|List of locators used to discover members of the distributed system. Supply all locators as comma-separated host:port values.|
|-bind-address|The address to which this peer binds for receiving peer-to-peer messages. By default gfxd uses the hostname, or localhost if the hostname points to a local loopback address.|
|-prop-name|Any other RowStore distributed system property.|

<a id="reference_FF886BB14E5949B79E47AC334D23EEE5__section_373A5D6CDE984CC49A03632C63252F2E"></a>
##Description

<a href="../../disk_storage/handling_missing_disk_stores.html#handling_missing_disk_stores" class="xref" title="Use the snappy-shell list-missing-disk-stores command to find disk stores with the latest persisted data. Use snappy-shell revoke-missing-disk-stores to stop waiting for the data when you cannot bring it online.">Handling Missing Disk Stores</a> provides more details about listing and revoking missing disk stores.

<a id="reference_FF886BB14E5949B79E47AC334D23EEE5__section_AFA4A7ACB7BA4CD58E33C8711B607AAD"></a>

##Example

``` pre
snappy-shell list-missing-disk-stores -mcast-port=10334
Connecting to distributed system: mcast=/239.192.81.1:10334
1f811502-f126-4ce4-9839-9549335b734d [curwen.local:/Users/yozie/snappydata/rowstore/SnappyData_RowStore_13_bNNNNN_platform/server2/./datadictionary]
```
