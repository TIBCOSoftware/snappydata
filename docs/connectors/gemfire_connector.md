# SnappyData GemFire Connector

<ent>This feature is available only in the Enterprise version of SnappyData. </br></ent>

## Overview
The SnappyData GemFire Connector allows SnappyData/Spark programs to read-from/write-into data regions in GemFire clusters. Applications can connect to one or more GemFire clusters, expose GemFire regions as SnappyData tables/Spark DataFrames, run complex SQL queries on data stored in GemFire and save SnappyData tables onto GemFire regions. The connector is designed to execute in a highly parallelized manner targeting GemFire partitioned datasets (buckets) for the highest possible performance. 

By exposing GemFire regions as Spark DataFrames, applications can benefit from the analytic features in SnappyData such as, flexible data transformations, analytics and moving data from/into almost all modern data stores.

### Features

 - Expose GemFire regions as SnappyData external tables

 - Run SQL queries on GemFire regions from SnappyData

 - Support joins on GemFire regions from SnappyData

 - Save SnappyData tables/DataFrames to GemFire

 - Support for POJOs as well as native support for GemFire PDX

 - Push query predicates and projection down to GemFire and use OQL for query execution

 - Query federation across multiple GemFire clusters

### Version and Compatibility

SnappyData GemFire Connector supports Spark 2.1 and has been tested with GemFire 8.2 or later.

## Quick Start Guide

This Quick Start guide explains, how to start a GemFire cluster, load some data onto a partitioned region, access this region as an SQL table, replicate to a SnappyData column table, and then run queries on both GemFire and SnappyData tables.

### Setting SnappyData GemFire Connector

!!! Note

	* This document assumes that you have basic knowledge of GemFire.

	* Ensure that you have [GemFire version 8.2 or later](https://network.pivotal.io/products/pivotal-gemfire#/releases/4375) installed and running.


The following section provides instructions to get a two-node GemFire cluster running and to deploy the functions required by SnappyData to access GemFire.

**To start Gemfire cluster and deploy SnappyData Connector functions**

1.	Start the GemFire shell.

		$ <GemFire_home>/bin/gfsh
		gfsh> start locator --name=locator1 --port=55221
	You need to use a non-default port, as SnappyData uses the same defaults as GemFire.

2.	Start two data servers.

		gfsh>start server --name=server1
		--locators=localhost[55221]
		gfsh>start server --name=server2
		--locators=localhost[55221] --server-port=40405

3.	Create Region.

		gfsh>create region --name=GemRegion --type=PARTITION --key-constraint=java.lang.String --value-constraint=java.lang.String

4.	Add at least 10 entries in this region using the PUT command.

		gfsh> put --key=1 --value='James'      --region=/GemRegion
		gfsh> put --key=2 --value='Jim'        --region=/GemRegion
		gfsh> put --key=3 --value='James Bond' --region=/GemRegion
		gfsh> put --key=4 --value='007'        --region=/GemRegion

5.	Deploy these functions to the GemFire cluster.

		gfsh>deploy --jar=<SnappyData-home>/connectors/gfeFunctions-0.9.jar

	A two node Gemfire cluster is up and running with a region **GemRegion** and the added entries.

### Configuring the SnappyData Cluster for GemFire Connector

The SnappyData cluster must be configured with details of the GemFire cluster with which the connector interfaces.</br>The configuration details should be provided to both the SnappyData lead and server nodes.

1.	Modify the server and lead configuration files that are located at:

	* **<_SnappyData-home_>/conf/leads**
	* **<_SnappyData-home_>/conf/servers**

2.	Add the connector jar (connector-0.9.jar) to the classpath and configure the remote GemFire cluster (locators, the servers and lead files) as follows:

        localhost -locators=localhost:10334 -client-bind-address=localhost 
        -classpath= <SnappyData-home>/connectors/connector-0.9.jar
        -spark.gemfire-grid.\<UniqueID\>=localhost[55221] 

	Here, the UniqueID is a name assigned for the Grid. </br>

	For example, SnappyData GemFire connector can connect to multiple Grids for federated data access.

		-spark.gemfire-grid.gridOne=localhost[55221] -spark.gemfire-grid.gridTwo=localhost[65221]

3.	[Start the SnappyData cluster](../howto/start_snappy_cluster.md) using the following command:

		$ <SnappyData-home>/sbin/snappy-start-all.sh


### Accessing GemFire as an SQL Table to Run Queries

The following section provides instructions to access GemFire as an SQL table to run queries.

**To access GemFire as an SQL table.**

1.	Start the Snappy Shell. 

		$<SnappyData-home>/bin/snappy
		snappy> connect client 'localhost:1527';
        
2.	Register an external table in SnappyData pointing to the GemFire region.

		snappy> create external table GemTable using gemfire options(regionPath 'GemRegion', keyClass 'java.lang.String', valueClass 'java.lang.String') ;
	
    The schema is automatically inferred from the object data in GemFire:
    
		snappy> describe gemTable;
        snappy> select * from gemTable;

### Replicating to SnappyData Table and Running Join Queries

You can replicate the data in GemFire SQL table into a SnappyData table and then run join queries.

1.	Create a SnappyData table based on the external table that was created using GemFire. 

		snappy> create table SnappyDataTable using column as (select * from gemTable);
		snappy> select * from SnappyDataTable;

2.	Run join queries.

		snappy> select t1.key_Column, t1.value_Column, t2.value_Column from GemTable t1, SnappyDataTable t2 where t1.key_Column = t2.key_Column;


## Initializing the GemFire Connector

SnappyData uses a set of functions that are deployed in the GemFire cluster, to interact with the cluster, for accessing metadata, runnning queries, and accessing data in GemFire. You must deploy the SnappyData GemFire Connector's jar that is **gemfire-function jar** into the GemFire cluster to enable the connector functionality.

### Enabling Connector Functionality
To enable the connector functionality,  you must deploy the SnappyData GemFire Connector's **gemfire-function** jar.

Execute the following to deploy the **gemfire-function** jar:

```
Deploy SnappyData GemFire Connector's gemfire-function jar (`gfeFunct
ions-1.0.2.jar`):
gfsh>deploy --jar=<SnappyData Product Home>/snappy-connectors/gemfire-connector/gfeFunctions/build-artifacts/scala-2.11/libs/gfeFunctions-1.0.2.jar

```

### Executing Queries with GemFire Connector

During the query execution, snappydata passes the names of attributes and filter conditions to the GemFire cluster, to prune the data that is fetched from GemFire.

For example, if you query for only attribute A from a GemFire Region value and that too of only those Region values which meet the filter condition, then instead of fetching the complete value only the pruned data needs to be sent to SnappyData.

For this purpose by default SnappyData relies on OQL of GemFire to prune the data. However, you can write custom **QueryExecutor** to retrieve the pruned data from GemFire. This is done by implementing **QueryExecutor** interface.
The **QueryExecutor** implementation should be packaged in a jar which is loaded by SnappyData using **ServiceLoader** API of java. As a part of the contract, the jar should include the following file with the path described:</br>**META-INF/services/io.snappydata.spark.gemfire.connector.query.QueryExecutor**</br>This file should contain the fully qualified class name of the custom **QueryExecutor**.

!!!Note
	The name of the file should be **io.snappydata.spark.gemfire.connector.query.QueryExecutor**.

This jar needs to be deployed on the GemFire cluster using **gfsh**.

!!!Attention
	It is important that as part of deployment, **gfeFunctions.jar** must be deployed first and then the jars containing custom **QueryExecutors**.

Following is an example of the implementation of **QueryExecutor** interface.

```
package io.snappydata.spark.gemfire.connector.query;

import java.util.Iterator;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.pdx.PdxInstance;
import io.snappydata.spark.gemfire.connector.internal.gemfirefunctions.shared.filter.Filter;

  public interface QueryExecutor<T, S> {
    /**
     *
     * @param region
     * @param requiredAttributes
     * @param filter
     * @param isPdxData
     * @return A stateful object which will be passed to the querying apis on every invocation.
     * The idea is that if the user can prepare an object which could be used for every bucket, it will be
     * efficient. So if a compiled Query object is prepared at the start of evaluation, then every bucket can
     * use this Query object instead of creating a compiled query object from string every time.
     */
    public S initStatefulObject(Region region, String[] requiredAttributes,
        T filter, boolean isPdxData);

    /**
     *
     * @return any String which uniquely identifies the Executor
     */
    public String getUniqueID();

    /**
     * This method will be invoked if the query needs a single field projection
     * User can return null in which case default implementation using OQL will be used
     * @param region Region on which query is to be performed
     * @param requiredAttribute projection which need to be returned
     * @param filter The desired format of the Filter condition to filter out relevant rows
     * @param forBucket Bucket ID on which the query is to be performed. will be -1 for replicated region
     * @param context RegionFunctionContext which can provide metadata to get handle of BucketRegion
     * @param  statefulObject which can be used across all buckets eg a compiled Query object.
     * @return An Iterator over the projected columns
     */
    public Iterator<Object> executeSingleProjectionQuery(Region region, String requiredAttribute, T filter,
        int forBucket, RegionFunctionContext context, S statefulObject);

  /**
   * This method will be invoked if the query needs multiple fields projection
   * User can return null in which case default implementation using OQL will be used
   * @param region Region on which query is to be performed
   * @param requiredAttributes A String array containing field names which need to be projected
   * @param filter The desired format of the Filter condition to filter out relevant rows
   * @param forBucket Bucket ID on which the query is to be performed. will be -1 for replicated region
   * @param context RegionFunctionContext which can provide metadata to get handle of BucketRegion
   * @param holder An instance of MultiProjectionHolder which should be used to store the projections during
   *               iteration. It is a wrapper over object[] intended to avoid creation of Object[] during ietaration
   * @param  statefulObject which can be used across all buckets eg a compiled Query object.
   * @return An Iterator of MultiProjectionHolder which contains the projected columns
   */
  public Iterator<MultiProjectionHolder> executeMultiProjectionQuery(Region region, String[] requiredAttributes,
      T filter, int forBucket, RegionFunctionContext context, MultiProjectionHolder holder, S statefulObject);

  /**
   * This method is invoked if the region contains PdxInstances. User needs to return the filtered PdxInstances
   * without applying projection, which will be applied internally . This will ensure that projected columns
   * do not get unnecessarily deserialized
   * User can return null in which case default implementation using OQL will be used
   * @param region Region on which query is to be performed
   * @param filter The desired format of the Filter condition to filter out relevant rows
   * @param forBucket Bucket ID on which the query is to be performed. will be -1 for replicated region
   * @param context RegionFunctionContext which can provide metadata to get handle of BucketRegion
   * @param  statefulObject which can be used across all buckets eg a compiled Query object.
   * @return Iterator over PdxInstance
   */
  public Iterator<PdxInstance> filteredPdxData(Region region,  T filter,
      int forBucket, RegionFunctionContext context, S statefulObject);

  /**
   *
   * @param filters Filter objects which need to be transformed by the user in the way it can be used in querying
   * @return transformed filter
   */
  public T transformFilter(Filter[] filters);
}

```

The working of **QueryExecutor** can be broken as follows:

-	[Converting the filter condition](#convertfilter) into a desired form.
-	[Initializing a stateful context object](#initializestatefulcontext) which avoids recreating a reusable unchanging object during data fetch on the individual buckets.
-	[Retrieval of data for Region](#retrievedataregion) as a whole, in case of replicated region, or for individual buckets, in case of partitioned region.

<a id= convertfilter> </a>
#### Convert Filter Condition
During execution, SnappyData checks with the available **QueryExecutor** by invoking the **transformFilter** method, if the filter is present.
In case the **QueryExecutor** is capable of handling it, a transformed filter is returned which can be used for data retrieval or else an **UnsupportedOperationException** is shown.

<a id= initializestatefulcontext> </a>
#### Initialize Stateful Context Object
If a transformed value is returned, then **initStatefulObject** method is invoked. You can utilize this to instantiate a reusable object such as, a parsed QueryPlan or a compiled structure or can simply return null. At this stage too, if the query cannot be handled, **UnsupportedOperationException** is shown.

<a id= retrievedataregion> </a>
#### Retrieve Data for Region
Next step is invocation of the appropriate method to get Iterator on the pruned data. This method can get invoked multiple times based on the number of partitioned region buckets that is operated upon.

-	If the region contains **pdx** instances, then the method **filteredPdxData** is invoked.

	!!!Note
		This method requires the user to return the iterator on valid Pdx instances. The relevant attributes are extracted by the framework.

-	If the region contains POJOs, then depending upon single or multiple attributes, one of the following method is invoked:
	- **executeSingleProjectionQuery** 
	- **executeMultiProjectionQuery***

<a id= configuresnappyforgemfire> </a>
### Configuring the SnappyData Cluster for GemFire Connector

The following configurations can be set in SnappyData Cluster for GemFire Connector:

-	[Customize DistributedSystem in the cluster with additional attributes](#customize_distributed_system)
-	[Specify Static Locators](#specifystatlocator)
-	[Dynamic Discovery of Grid for a Region](#dynamicdiscoverygridregion)

<a id= customize_distributed_system> </a>
#### Customizing the **DistributedSystem** of SnappyData Cluster at Startup

You can configure the **DistributedSystem** with additional attributes by implementing the following interface:

```
package io.snappydata.spark.gemfire.connector.dsinit;
/**
 * @param @link{DSConfig} instance which can be used to configure the DistributedSystem properties at the
 * time of snappydata startup
 */
public interface DistributedSystemInitializer {

  public void configure(DSConfig config);
}
```

Any required properties of the **DistributedSystem** can be configured via the **DSConfig** instance passed.
The **DistributedSystemInitializer** implementation needs to be packaged in a jar which is loaded by SnappyData using ServiceLoader API of java. The jar should include the following file along with the path as described:

**META-INF/services/io.snappydata.spark.gemfire.connector.dsinit.DistributedSystemInitializer**

This file should contain the fully qualified class name of the custom DistributedSystemInitializer

!!!Note
	The name of the file should be **io.snappydata.spark.gemfire.connector.dsinit.DistributedSystemInitializer**

The connector interacts with the GemFire cluster. Therefore, you should configure the SnappyData cluster with the details of Gemfire cluster. The configuration details must be provided in both the SnappyData lead and server nodes at the following locations:
-	**SnappyData-Home directory/conf/leads**
-	**SnappyData-Home directory/conf/servers **

Modify the servers and leads configuration file to add the connector jar (**connector-1.0.0.jar**) and the **person.jar** in the classpath as well as to configure the remote GemFire cluster locator.

<a id= specifystatlocator> </a>
#### Specifying Static Locators

To statically specify the running locators of the GemFire cluster, set the property as follows, where **uniqueIDForGrid** is any unique identifier key:

		-snappydata.connector.gemfire-grid.\<uniqueIDForGrid\>=localhost[55221]


Following is a sample from the servers and leads file:

```
localhost -locators=localhost:10334 -client-bind-address=localhost -client-port=1528 -heap-size=20g
-classpath=<SnappyData-Product-Home>/snappy-connectors/gemfire-connector/connector/build-artifacts/scala-2.11/libs/connector_2.11-1.0.2.jar:<path-to-jar>/person.jar
-snappydata.connector.gemfire-grid.<uniqueIDForGrid>=localhost[55221]
```

<a id= dynamicdiscoverygridregion> </a>
#### Dynamically Discovering Grid for a Region

Instead of specifying the locators via the property, it is possible to provide custom logic for discovery of the grid for the region by implementing **GridResolver** trait as follows:

```
trait GridResolver {

  /**
    * Optional method to identify the locators and delegate to SnappyData for creating connection 
    * pool using [[GridHelper]]. Invoked once in the lifecycle.
    * @param gfeGridProps
    * @param gridHelper
    */
  def initConnections(gfeGridProps: java.util.Map[String, String], gridHelper: GridHelper): Unit

  /**
    * Resolve the grid for the region or return null if unable to do so
    * @param regionName
    * @param gfeGridProps
    * @param gridHelper
    * @param userOptions
    * @return the grid name ( the name of the connection pool) to use
    */
  def getGridForRegion(regionName: String, gfeGridProps: java.util.Map[String, String],
      gridHelper: GridHelper, userOptions: java.util.Map[String, String]): String

  /**
    *
    * @return a String identifying the resolver uniquely
    */
  def getUniqueID: String
}
```
SnappyData attempts to resolve the grid for the region using existing **ConnectionPools**. If it is not successful, it checks with the available resolvers by invoking **getGridForRegion**. The resolver returns null, if it cannot resolve the grid.

The **GridResolver** implementation should be packaged in a jar which is loaded by SnappyData using **ServiceLoader** API of java. The jar should also include the following file along with the described path:</br>**META-INF/services/io.snappydata.spark.gemfire.connector.grids.GridResolver**</br>
This file should contain the fully qualified class name of the custom **GridResolver**.

!!!Note
	The name of the file should be **io.snappydata.spark.gemfire.connector.grids.GridResolver**.

### Initializing the GemFire Connector in SnappyData
To initialize the GemFire connector and enable its functions in SnappyData, you must include the following import statement before creating the external table:

```
import io.snappydata.spark.gemfire.connector
```

## Exposing GemFire Regions as External Tables

You can use any the following options to expose GemFire regions as external tables:
*	[Expose GemFire PDX Regions as External Tables](#gemfirepdx)
*	[Expose Regions Containing POJOs as External Tables](#gemfirepojo)
*	[Expose GemFire Region Using Dataframe based External Tables](#gemfiredataframe)


<a id= gemfirepdx> </a>
### Exposing GemFire PDX Regions as External Tables
You can create an external table that represents a GemFire region which stores PDX instances. The SnappyData schema for this external table is derived from the PDXType. Here, the GemFire region is already populated with data and SnappyData infers the schema based on the inspection of the PDX types.
The following syntax creates an external table that represents a GemFire region which stores PDX instances.

```
val externalBsegTable = snc.createExternalTable("bsegInGem", 
     "gemfire",
     Map[String, String]("region.path" -> "bseg1", "data.as.pdx" -> "true"))
```     

The SnappyData external table schema for the GemFire region can optionally include the GemFire region key as a column in the table. To enable this, the **key.class ** attribute should be set when you create the table as shown in the following example:

```
val externalBsegTable = snc.createExternalTable("bsegInGem",
   "gemfire",
   Map[String, String]("region.path" -> "bseg1", "data.as.pdx" -> "true",
   "key.class" -> "java.lang.Long"     
     ))
```

<a id= gemfirepojo> </a>
### Exposing Regions containing POJOs As External Tables

IN the following example, an external table is created using the getter methods on POJOs as SnappyData column names.
```
snc.createExternalTable(externalPersonsTable1, "gemfire", 
Map[String, String]("region.path" -> personsRegionName, "value.class" -> "load.Person"))
``` 
As in the previous case, if the GemFire key field has to be included as a column, then the **Key.class** attribute has to be passed in as an option.

```
snc.createExternalTable(externalPersonsTable1, "gemfire", Map[String, String]("region.path" -> personsRegionName, 
"value.class" -> "load.Person"), "key.class" -> "java.lang.Long"))
```

<a id= gemfiredataframe> </a>
### Expose GemFire Region Using Dataframe based External Tables

In the following example, a DataFrame is used to create an external table **bsegTable**, with the schema same as that of  DataFrame bsegDF. The primary key column name should be specified for this to work correctly. In the example above, it is assumed that the dataframe contains a column named "id1".
```
bsegDF.write.format("gemfire").  
      option("region.path", "bseg1").
      option("primary.key.column.name", "id1").
      option("pdx.identity.fields", "id1").
      saveAsTable("bsegTable")
```
In the above example,  The *pdx.identity.fields* specification has a huge impact on performance within GemFire as it tells GemFire to use only the specified field for computing hashCode & equality for PdxInstance.

### Controlling the schema of the external table
By default all the pdx fields ( of pdx instances) or the getter methods (of the POJOs) define the schema of the external table. But it is possible to control the schema by excluding columns as per requirement.
This is done by implementing the following trait.
```scala
package io.snappydata.spark.gemfire.connector.lifecycle
import org.apache.spark.sql.types.StructType

trait ColumnSelector {
  def selectColumns(gridName: Option[String], regionName: String,
      allColumns: StructType): StructType
}

```
Based on the requirement , a new struct type containing fewer columns can be returned. It is to be noted that new StructType returned can only have subset of the columns passed. No new columns should be added.

The ColumnSelector implemetation needs to be added to the startup classpath of the SnappyDataCluster.
At the time of table creation , an option with key = **column.selector** and value as the fully qualified class name of the ColumnSelector implementation class, needs to be passed

### Other optional configuration parameters
1) **max.rows.restrict** 

  This is used to restrict the number of rows fetched from external table when a query of type 
   **select * from external_table** is fired. This restriction if specified will be applied to query without any filter, limit, aggregate function, projection such that it tends to bring all the rows from the external region. Pls note that this restriction is not applied in case of DDL statement like
   **create table X using row | column as select * from external_table**

2) **column.selector** 

  This is used to control the columns to be present in the schema as described [above](https://github.com/SnappyDataInc/snappy-connectors/blob/master/gemfire-connector/doc/3_loading.md#controlling-the-schema-of-the-external-table)

3) **max.buckets.per.partition** 

  This property is used to control the concurrency & number of tasks created to fetch data from external region. This property is meaningful only for Partitioned Region. Detailed explanation is found [below](https://github.com/SnappyDataInc/snappy-connectors/blob/master/gemfire-connector/doc/3_loading.md#controlling-task-concurrency-in-snappydata-when-accessing-gemfire)
  
4) **security.username**  and **security.password**

  By default the external table is created & queried using the credential of the current user. For example user X creates external table, and if user Y is querying then credentials of user Y will be fetched to data. But if the parameters **security.username**  and **security.password** are passed then those credentials will be used to create external table & for querying the data irrespective of the current user.
  
5) **pdxtype.maxscan**

  This parameter determines the maximum number of entries from the region which will be scanned to completely determine the schema of the external table from the Pdx Instances stored in the region. The default value is 100

### Expose GemFire Regions as RDDs
Invoking the gemfireRegion method on the SparkContext in SnappyData exposes the full data set of a GemFire region as a Spark RDD.
The same API exposes both replicated and partitioned region as RDDs. 

```
scala> val rdd = sc.gemfireRegion[String, String]("gemTable1")

scala> rdd.foreach(println)
(1,one)
(3,three)
(2,two)
```

Note: It is important to specifiy the correct type for both the region key and value, otherwise a ClassCastException is encountered when the RDD is used.
### Controlling Task Concurrency in SnappyData When Accessing GemFire 
There are two types of regions in GemFire viz.  **replicated**, and
**partitioned**. All the data for a replicated region is present on every server that has the region defined. A GemFire partitioned region splits up its data across the servers that define the partitioned region. When operating with replicated regions, there is only a single RDD partition representing the replicated region in SnappyData (since there is only one data bucket for the replicated region) In contrast to this, a GemFire partitioned region can be represented by a configurable number of RDD partitions in SnappyData. Choosing the number of RDD partitions directly controls task concurrency in SnappyData while running queries on GemFire regions. By default, the GemFire connector figures out the number of buckets per GemFire server and assigns partitions to each server, and uses a default value of maximum 3 buckets per partition. Users can configure the "max.buckets.per.partition" attribute to change this value. When queries are fired on the external table, the degree of parallelism in query execution is directly proportional to the number of RDD partitions that represents the table.

The example below shows how to configure the RDD partitions count for an external table representing a GemFire region
```
import io.snappydata.spark.gemfire.connector._

val externalBsegTable = snc.createExternalTable("bsegInGem",
   "gemfire",
   Map[String, String]("region.path" -> "bseg1", "data.as.pdx" -> "true",
   "key.class" -> "java.lang.Long" , "max.buckets.per.partition" -> "5"    
     ))
 
```

Next: [Saving Data To GemFire](5_savingtogemfire.md)
