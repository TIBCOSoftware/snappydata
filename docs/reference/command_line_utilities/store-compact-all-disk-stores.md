# compact-all-disk-stores

Perform online compaction of SnappyData disk stores.

##Syntax

``` pre
snappy compact-all-disk-stores
 [-mcast-port=<port>] [-mcast-address=<address>]
 [-locators=<addresses>] [-bind-address=<address>] [-<prop-name>=<prop-value>]*
```

The table describes options for snappy compact-all-disk-stores. If no multicast or locator options are specified on the command-line, then the command uses the <span class="ph filepath">gemfirexd.properties</span> file (if available) to determine the distributed system to which it should connect.

|Option|Description|
|-|-|
|-mcast-port|</br>Multicast port used to communicate with other members of the distributed system. If zero, multicast is not used for member discovery (specify `-locators` instead).</br>Valid values are in the range 0–65535, with a default value of 10334.|
|-mcast-address|</br>Multicast address used to discover other members of the distributed system. This value is used only if the `-locators` option is not specified.</br>The default multicast address is 239.192.81.1.|
|-locators|</br>List of locators used to discover members of the distributed system. Supply all locators as comma-separated host:port values.|
|-bind-address|The address to which this peer binds for receiving peer-to-peer messages. By default `gfxd` uses the hostname, or localhost if the hostname points to a local loopback address.|
|-&lt;prop-name&gt;=&lt;prop-value&gt;|</br>Any other SnappyData distributed system property.|


<a id="reference_13F8B5AFCD9049E380715D2EF0E33BDC__section_050663B03C0A4C42B07B4C5F69EAC95D"></a>
##Description

When a CRUD operation is performed on a persistent/overflow table, the data is written to the log files. Any pre-existing operation record for the same row becomes obsolete, and SnappyData marks it as garbage. It compacts an old operation log by copying all non-garbage records into the current log and discarding the old files.

Manual compaction can be done for online and offline disk stores. For the online disk store, the current operation log is not available for compaction, no matter how much garbage it contains.

Offline compaction runs essentially in the same way, but without the incoming CRUD operations. Also, because there is no current open log, the compaction creates a new one to get started.

<a id="reference_13F8B5AFCD9049E380715D2EF0E33BDC__section_5275248D56414A51AFB4492DE783E7F1"></a>

##Online Compaction


To run manual online compaction, allow-force-compaction should be true. You can run manual online compaction at any time while the system is running. Oplogs eligible for compaction based on the *compaction-threshold* are compacted into the current oplog.
