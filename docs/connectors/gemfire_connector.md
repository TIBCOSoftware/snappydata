# How to use the SnappyData GemFire Connector

<ent>This feature is available only in the Enterprise version of SnappyData. </br></ent>

The SnappyData GemFire Connector allows SnappyData/Spark programs to read-from and write-into data regions in the GemFire cluster. Applications can connect to one or more GemFire cluster, expose GemFire regions as SnappyData Tables/Spark DataFrames, run complex SQL queries on data stored in GemFire, and save SnappyData Tables out to GemFire regions. </br>The connector is designed to execute in a highly parallelized manner targeting GemFire partitioned datasets(buckets) for the highest possible performance.

By exposing GemFire regions as Spark DataFrames, applications can benefit from the analytic features in SnappyData such as; flexible data transformations, analytics and moving data from/into almost all modern data stores.

## Features

 - Expose GemFire regions as SnappyData external tables

 - Run SQL queries on GemFire regions from SnappyData

 - Support joins on GemFire regions from SnappyData

 - Save SnappyData tables/DataFrames to GemFire

 - Support for POJOs as well as native support for GemFire PDX

 - Push query predicates and projection down to GemFire and use OQL for query execution

 - Query federation across multiple GemFire clusters

## Version and Compatibility

SnappyData GemFire Connector supports Spark 2.1 and has been tested with GemFire 8.2 or later.

## Quick Start Steps

In this quickstart, we explain how to start a GemFire cluster, load some data into a partitioned region, access this region as an SQL table, replicate to a SnappyData column table, and then run queries on both GemFire and SnappyData tables.

## Setup GemFire 8.2 Cluster and Deploy SnappyData Connector Functions

!!! Note

	* This document assumes that you have basic knowledge of GemFire.

	* Ensure that you have [GemFire version 8.2 or later](https://network.pivotal.io/products/pivotal-gemfire#/releases/4375) installed and running.

The following section provides instructions to get a two-node GemFire cluster running, and to deploy the functions required by SnappyData to access GemFire.

### Start the GemFire Shell

``` bash
$ <GemFire_home>/bin/gfsh

gfsh> start locator --name=locator1 --port=55221
// You need to use a non-default port, as SnappyData uses the same defaults as GemFire.

// Start two data servers
gfsh>start server --name=server1 
--locators=localhost[55221]

gfsh>start server --name=server2 
--locators=localhost[55221] --server-port=40405 

gfsh>create region --name=GemRegion --type=PARTITION --key-constraint=java.lang.String --value-constraint=java.lang.String

// Next, add at least 10 entries in this region using the PUT command.

gfsh> put --key=1 --value='James'      --region=/GemRegion
gfsh> put --key=2 --value='Jim'        --region=/GemRegion
gfsh> put --key=3 --value='James Bond' --region=/GemRegion
gfsh> put --key=4 --value='007'        --region=/GemRegion

// You have to deploy these functions to the GemFire cluster.

gfsh>deploy --jar=<SnappyData-home>/connectors/gfeFunctions-0.9.jar
```

A two node Gemfire cluster is up and running with a region **GemRegion** and the added entries.

### Configuring the SnappyData Cluster

The SnappyData cluster needs to be configured with details of the GemFire cluster with which the connector interfaces.
</br>The configuration details should be provided to both the SnappyData lead and server nodes.

Modify the server and lead configuration files that are located at:

* **<_SnappyData-home_>/conf/leads** 
* **<_SnappyData-home_>/conf/servers**

Add the connector jar (connector-0.9.jar) to the classpath and configure the remote GemFire cluster (locators, the servers and lead files) as follows:

```bash
localhost -locators=localhost:10334 -client-bind-address=localhost 
-classpath= <SnappyData-home>/connectors/connector-0.9.jar
-spark.gemfire-grid.\<UniqueID\>=localhost[55221] 
```
Here, the UniqueID is a name assigned for the Grid. </br>

For example, SnappyData GemFire connector can connect to multiple Grids for federated data access.
```
-spark.gemfire-grid.gridOne=localhost[55221] -spark.gemfire-grid.gridTwo=localhost[65221]
```

Finally, [start the SnappyData cluster](../howto/start_snappy_cluster.md) using the following command:

```bash
$ <SnappyData-home>/sbin/snappy-start-all.sh
```

### Access GemFire as an SQL table and Run Queries

Start the Snappy Shell using the command,

```bash
$ <SnappyData-home>/bin/snappy

snappy> connect client 'localhost:1527';

// Register an external table in SnappyData pointing to the GemFire region

snappy> create external table GemTable using gemfire options(regionPath 'GemRegion', keyClass 'java.lang.String', valueClass 'java.lang.String') ;

// The schema is automatically inferred from the object data in GemFire
snappy> describe gemTable;

snappy> select * from gemTable;
```

### Copy to SnappyData Column Table, Run Queries, Join with GemFire

```bash
snappy> create table SnappyDataTable using column as (select * from gemTable);

snappy> select * from SnappyDataTable;

// Join Snappy column table with GemFire data
snappy> select t1.keyColumn, t1.valueColumn, t2.valueColumn from GemTable t1, SnappyDataTable t2 where t1.keyColumn = t2.keyColumn;
```
