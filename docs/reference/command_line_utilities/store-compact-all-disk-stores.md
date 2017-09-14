# compact-all-disk-stores

Perform online compaction of SnappyData disk stores.

## Syntax

``` pre
snappy compact-all-disk-stores==
  <-locators=<addresses>> [-bind-address=<address>] [-<prop-name>=<prop-value>]*
```

The table describes options for `snappy compact-all-disk-stores`. 

|Option|Description|
|--------|--------|
|locators|List of locators used to discover members of the distributed system. Supply all locators as comma-separated host:port values. The port is the `peer-discovery-port` used when starting the cluster (default 10334). This is a mandatory field.|
|bind-address    |The address to which this peer binds for receiving peer-to-peer messages. By default `snappy` uses the hostname, or localhost if the hostname points to a local loopback address.|
|prop-name</br> prop-value    |Any other SnappyData distributed system property.|

## Description

When a CRUD operation is performed on a persistent/overflow table, the data is written to the log files. Any pre-existing operation record for the same row becomes obsolete, and SnappyData marks it as garbage. It compacts an old operation log by copying all non-garbage records into the current log and discarding the old files.

Manual compaction can be done for online and offline disk stores. For the online disk store, the current operation log is not available for compaction, no matter how much garbage it contains.

Offline compaction runs essentially in the same way, but without the incoming CRUD operations. Also, because there is no current open log, the compaction creates a new one to get started.

## Online Compaction

To run manual online compaction, ALLOWFORCECOMPACTION should be set to true while [creating a diskstore](/reference/sql_reference/create-diskstore.md)
You can run manual online compaction at any time while the system is running. Oplogs eligible for compaction based on the COMPACTIONTHRESHOLD are compacted into the current oplog.
