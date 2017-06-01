# revoke-missing-disk-store
Instruct SnappyData members to stop waiting for a disk store to become available.

##Syntax

``` pre
snappy revoke-missing-disk-store <disk-store-id>
  [-mcast-port=<port>]
  [-mcast-address=<address>]
  [-locators=<addresses>] 
        [-bind-address=<address>] 
  [-<prop-name>=<prop-value>]*
```

The table describes options and arguments for snappy revoke-missing-disk-store. If no multicast or locator options are specified on the command-line, the command uses the <span class="ph filepath">gemfirexd.properties</span> file (if available) to determine the distributed system to which it should connect.

|Option<|Description|
|-|-|
|&lt;disk-store-id&gt;|(Required.) Specifies the unique ID of the disk store to revoke. <!--You can obtain the ID from the output of  [show-disk-store-metadata](store-show-disk-store-metadata.md#reference_FF886BB14E5949B79E47AC334D23EEE5)-->|
|-mcast-port|</br>Multicast port used to communicate with other members of the distributed system. If zero, multicast is not used for member discovery (specify `-locators` instead).</br>Valid values are in the range 0–65535, with a default value of 10334.|
|-mcast-address|</br>Multicast address used to discover other members of the distributed system. This value is used only if the `-locators` option is not specified.</br>The default multicast address is 239.192.81.1.|
|-locators|</br>List of locators used to discover members of the distributed system. Supply all locators as comma-separated host:port values.|
|-bind-address|The address to which this peer binds for receiving peer-to-peer messages. By default gfxd uses the hostname, or localhost if the hostname points to a local loopback address.|
|-prop-name|</br>Any other SnappyData distributed system property.|

<a id="reference_FF886BB14E5949B79E47AC334D23EEE5__section_373A5D6CDE984CC49A03632C63252F2E"></a>
##Description

[Handling Missing Disk Stores](../../concepts/tables/persisting_table_data/handling_missing_disk_stores.md#handling_missing_disk_stores) provides more details about listing and revoking missing disk stores.

<a id="reference_FF886BB14E5949B79E47AC334D23EEE5__section_AFA4A7ACB7BA4CD58E33C8711B607AAD"></a>

##Example

This command first lists the missing disk store:

``` pre
snappy list-missing-disk-stores -mcast-port=10334
Connecting to distributed system: mcast=/239.192.81.1:10334
1f811502-f126-4ce4-9839-9549335b734d [curwen.local:/Users/yozie/snappydata/rowstore/SnappyData_RowStore_13_bNNNNN_platform/server2/./datadictionary]
```

Next, `snappy` revokes the missing disk store if more recent data is available:

``` pre
snappy revoke-missing-disk-store 1f811502-f126-4ce4-9839-9549335b734d -mcast-port=10334
Connecting to distributed system: mcast=/239.192.81.1:10334
revocation was successful and no disk stores are now missing
```

Finally, `snappy` verifies that no disk stores are missing:

``` pre
snappy list-missing-disk-stores -mcast-port=10334
Connecting to distributed system: mcast=/239.192.81.1:10334
The distributed system did not have any missing disk stores
```
