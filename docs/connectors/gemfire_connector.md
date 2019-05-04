# TIBCO ComputeDB GemFire Connector

<ent>This feature is available only in the TIBCO ComputeDB - Enterprise Edition. </br></ent>

## Overview
The TIBCO ComputeDB GemFire Connector allows TIBCO ComputeDB/Spark programs to read from data regions as well as write into data regions within GemFire clusters. You can connect the applications to one or more GemFire clusters, expose GemFire regions as TIBCO ComputeDB tables/Spark DataFrames, run complex SQL queries on data stored in GemFire and save TIBCO ComputeDB tables onto GemFire regions. The connector is designed to execute in a highly parallelized manner targeting GemFire partitioned datasets (buckets) for the highest possible performance.

By exposing GemFire regions as Spark DataFrames, applications can benefit from the analytic features in TIBCO ComputeDB such as, flexible data transformations, analytics and moving data from/into almost all modern data stores.

### Features

 - Expose GemFire regions as TIBCO ComputeDB external tables

 - Run SQL queries on GemFire regions from TIBCO ComputeDB

 - Support joins on GemFire regions from TIBCO ComputeDB

 - Save TIBCO ComputeDB tables/DataFrames to GemFire

 - Support for POJOs as well as native support for GemFire PDX

 - Push query predicates and projection down to GemFire and use OQL for query execution

 - Query federation across multiple GemFire clusters

### Version and Compatibility

TIBCO ComputeDB GemFire Connector supports Spark 2.1 and has been tested with GemFire 8.2 or later.

## Quick Start Guide

This Quick Start guide explains, how to start a GemFire cluster, load data onto a partitioned region, access this region as an SQL table, replicate to a TIBCO ComputeDB column table, and then run queries on both GemFire and TIBCO ComputeDB tables.

*	[Set TIBCO ComputeDB GemFire Connector](#setsnappygemfireconnector)
*	[Configure the TIBCO ComputeDB Cluster for GemFire Connector](#configuresnappyclustergemfire)
*	[Access GemFire as an SQL Table to Run Queries](#accessgemfireassql>)
*	[Replicate to TIBCO ComputeDB Table and Running Join Queries](#replicatesnappytable)

<a id= setsnappygemfireconnector> </a>
### Setting TIBCO ComputeDB GemFire Connector

The following prerequisites are required for setting up GemFire connector:

**Prerequisites** 

*	Basic knowledge of GemFire
*	[GemFire version 8.2 or later](https://network.pivotal.io/products/pivotal-gemfire#/releases/4375) installed and running.

The following section provides instructions to get a two-node GemFire cluster running and to deploy the functions required by TIBCO ComputeDB to access GemFire.

**To start GemFire cluster and deploy TIBCO ComputeDB Connector functions**

1.	Start the GemFire shell.

		$ <GemFire_home>/bin/gfsh
		gfsh> start locator --name=locator1 --port=55221
	You need to use a non-default port, as TIBCO ComputeDB uses the same defaults as GemFire.

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

		gfsh>deploy --jar=<TIBCO ComputeDB_home>/connectors/gfeFunctions-0.9.jar

	A two node GemFire cluster is up and running with a region **GemRegion** and the added entries.

<a id= configuresnappyclustergemfire> </a>
### Configuring the TIBCO ComputeDB Cluster for GemFire Connector

The TIBCO ComputeDB cluster must be configured with details of the GemFire cluster with which the connector interfaces.</br>The configuration details should be provided to both the TIBCO ComputeDB lead and server nodes.

1.	Modify the server and lead configuration files that are located at:

	* **<TIBCO ComputeDB_home_>/conf/leads**
	* **<TIBCO ComputeDB_home_>/conf/servers**

2.	Add the connector jar (connector-0.9.jar) to the classpath and configure the remote GemFire cluster (locators, the servers and lead files) as follows:

        localhost -locators=localhost:10334 -client-bind-address=localhost 
        -classpath= <TIBCO ComputeDB_home>/connectors/connector-0.9.jar
        -spark.gemfire-grid.\<UniqueID\>=localhost[55221] 

	Here, the UniqueID is a name assigned for the Grid. </br>

	For example, TIBCO ComputeDB GemFire connector can connect to multiple Grids for federated data access.

		-spark.gemfire-grid.gridOne=localhost[55221] -spark.gemfire-grid.gridTwo=localhost[65221]

3.	[Start the TIBCO ComputeDB cluster](../howto/start_snappy_cluster.md) using the following command:

		$ <TIBCO ComputeDB_home>/sbin/snappy-start-all.sh

<a id= accessgemfireassql> </a>
### Accessing GemFire as an SQL Table to Run Queries

The following section provides instructions to access GemFire as an SQL table to run queries.

**To access GemFire as an SQL table.**

1.	Start the Snappy Shell. 

		$<TIBCO ComputeDB_home>/bin/snappy
		snappy> connect client 'localhost:1527';
        
2.	Register an external table in TIBCO ComputeDB pointing to the GemFire region.

		snappy> create external table GemTable using gemfire options(regionPath 'GemRegion', keyClass 'java.lang.String', valueClass 'java.lang.String') ;
	
    The schema is automatically inferred from the object data in GemFire:
    
		snappy> describe gemTable;
        snappy> select * from gemTable;

<a id= replicatesnappytable> </a>
### Replicating to TIBCO ComputeDB Table and Running Join Queries

You can replicate the data in GemFire SQL table into a TIBCO ComputeDB table and then run join queries.

1.	Create a TIBCO ComputeDB table based on the external table that was created using GemFire. 

		snappy> create table TIBCO ComputeDBTable using column as (select * from gemTable);
		snappy> select * from TIBCO ComputeDBTable;

2.	Run join queries.

		snappy> select t1.key_Column, t1.value_Column, t2.value_Column from GemTable t1, TIBCO ComputeDBTable t2 where t1.key_Column = t2.key_Column;

<a id= initializegemfireconnector> </a>
## Initializing the GemFire Connector

TIBCO ComputeDB uses a set of functions that are deployed in the GemFire cluster, to interact with the cluster, for accessing metadata, runnning queries, and accessing data in GemFire. You must deploy the TIBCO ComputeDB GemFire Connector's jar that is **gemfire-function jar** into the GemFire cluster to enable the connector functionality.

### Enabling Connector Functionality
To enable the connector functionality,  you must deploy the TIBCO ComputeDB GemFire Connector's **gemfire-function** jar.

Execute the following to deploy the **gemfire-function** jar:

```
Deploy TIBCO ComputeDB GemFire Connector's gemfire-function jar (`gfeFunct
ions-0.9.2.1.jar`):
gfsh>deploy --jar=<TIBCO ComputeDB_Product_Home>//connectors/gfeFunctions-0.9.2.1.jar


```

### Executing Queries with GemFire Connector

During the query execution, TIBCO ComputeDB passes the names of attributes and filter conditions to the GemFire cluster, to prune the data that is fetched from GemFire.

For example, if you query for only attribute A from a GemFire Region value and that too of only those Region values which meet the filter condition, then instead of fetching the complete value only the pruned data needs to be sent to TIBCO ComputeDB.

For this purpose by default TIBCO ComputeDB relies on OQL of GemFire to prune the data. However, you can write custom **QueryExecutor** to retrieve the pruned data from GemFire. This is done by implementing **QueryExecutor** interface.
The **QueryExecutor** implementation should be packaged in a jar which is loaded by TIBCO ComputeDB using **ServiceLoader** API of java. As a part of the contract, the jar should include the following file with the path described:</br>**META-INF/services/io.snappydata.spark.gemfire.connector.query.QueryExecutor**</br>This file should contain the fully qualified class name of the custom **QueryExecutor**.

!!!Note
	The name of the file should be **io.snappydata.spark.gemfire.connector.query.QueryExecutor**.

This jar needs to be deployed on the GemFire cluster using **gfsh**.

!!!Attention
	It is important that as part of deployment, **gfeFunctions.jar** must be deployed first and then the jars containing custom **QueryExecutors**.

Following is an example of the implementation of **QueryExecutor** interface:

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
During execution, TIBCO ComputeDB checks with the available **QueryExecutor** by invoking the **transformFilter** method, if the filter is present.
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
### Configuring the TIBCO ComputeDB Cluster for GemFire Connector

The following configurations can be set in TIBCO ComputeDB Cluster for GemFire Connector:

-	[Customize DistributedSystem in the cluster with additional attributes](#customize_distributed_system)
-	[Specify Static Locators](#specifystatlocator)
-	[Dynamic Discovery of Grid for a Region](#dynamicdiscoverygridregion)

<a id= customize_distributed_system> </a>
#### Customizing the **DistributedSystem** of TIBCO ComputeDB Cluster at Startup

You can configure the **DistributedSystem** with additional attributes by implementing the following interface:

```
package io.snappydata.spark.gemfire.connector.dsinit;
/**
 * @param @link{DSConfig} instance which can be used to configure the DistributedSystem properties at the
 * time of TIBCO ComputeDB startup
 */
public interface DistributedSystemInitializer {

  public void configure(DSConfig config);
}
```

Any required properties of the **DistributedSystem** can be configured via the **DSConfig** instance passed.
The **DistributedSystemInitializer** implementation needs to be packaged in a jar which is loaded by TIBCO ComputeDB using ServiceLoader API of java. The jar should include the following file along with the path as described:

**META-INF/services/io.snappydata.spark.gemfire.connector.dsinit.DistributedSystemInitializer**

This file should contain the fully qualified class name of the custom DistributedSystemInitializer

!!!Note
	The name of the file should be **io.snappydata.spark.gemfire.connector.dsinit.DistributedSystemInitializer**

The connector interacts with the GemFire cluster. Therefore, you should configure the TIBCO ComputeDB cluster with the details of GemFire cluster. The configuration details must be provided in both the TIBCO ComputeDB lead and server nodes at the following locations:
-	**TIBCO ComputeDB_Home directory/conf/leads**
-	**TIBCO ComputeDB_Home directory/conf/servers **

Modify the servers and leads configuration file to add the connector jar (**connector-1.0.0.jar**) and the **person.jar** in the classpath as well as to configure the remote GemFire cluster locator.

<a id= specifystatlocator> </a>
#### Specifying Static Locators

To statically specify the running locators of the GemFire cluster, set the property as follows, where **uniqueIDForGrid** is any unique identifier key:

		-snappydata.connector.gemfire-grid.\<uniqueIDForGrid\>=localhost[55221]


Following is a sample from the servers and leads file:

```
localhost -locators=localhost:10334 -client-bind-address=localhost -client-port=1528 -heap-size=20g
-classpath=<TIBCO ComputeDB_Home>//connectors/connector_2.11-0.9.2.1.jar:<path-to-jar>/persons.jar
-snappydata.connector.gemfire-grid.<uniqueIDForGrid>=localhost[55221]
```

<a id= dynamicdiscoverygridregion> </a>
#### Dynamically Discovering Grid for a Region

Instead of specifying the locators via the property, it is possible to provide custom logic for discovery of the grid for the region by implementing **GridResolver** trait as follows:

```
trait GridResolver {

  /**
    * Optional method to identify the locators and delegate to TIBCO ComputeDB for creating connection 
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
TIBCO ComputeDB attempts to resolve the grid for the region using existing **ConnectionPools**. If it is not successful, it checks with the available resolvers by invoking **getGridForRegion**. The resolver returns null, if it cannot resolve the grid.

The **GridResolver** implementation should be packaged in a jar which is loaded by TIBCO ComputeDB using **ServiceLoader** API of java. The jar should also include the following file along with the described path:</br>**META-INF/services/io.snappydata.spark.gemfire.connector.grids.GridResolver**</br>
This file should contain the fully qualified class name of the custom **GridResolver**.

!!!Note
	The name of the file should be **io.snappydata.spark.gemfire.connector.grids.GridResolver**.

To initialize the GemFire connector and enable its functions in TIBCO ComputeDB, you must include the following import statement before creating the external table:

```
import io.snappydata.spark.gemfire.connector
```
<a id= accessingdatafromgemfire> </a>
## Accessing Data from GemFire
In TIBCO ComputeDB applications, you can create external tables that represent GemFire regions and run SQL queries against GemFire. For accesing data from GemFire, you must first expose the GemFire regions: 

You can use any the following options to expose GemFire regions:

*	[Expose GemFire PDX Regions as External Tables](#gemfirepdx)
*	[Expose Regions Containing POJOs as External Tables](#gemfirepojo)
*	[Expose GemFire Region Using Dataframe based External Tables](#gemfiredataframe)
*	[Expose GemFire Regions as RDDs](#gemfirerdds)

<a id= gemfirepdx> </a>
### Exposing GemFire PDX Regions as External Tables
You can create an external table that represents a GemFire region which stores PDX instances. The TIBCO ComputeDB schema for this external table is derived from the PDXType. Here, the GemFire region is already populated with data and TIBCO ComputeDB infers the schema based on the inspection of the PDX types.
The following syntax creates an external table that represents a GemFire region which stores PDX instances.

```
val externalBsegTable = snc.createExternalTable("bsegInGem", 
     "gemfire",
     Map[String, String]("region.path" -> "bseg1", "data.as.pdx" -> "true"))
```     

The TIBCO ComputeDB external table schema for the GemFire region can optionally include the GemFire region key as a column in the table. To enable this, the **key.class ** attribute should be set when you create the table as shown in the following example:

```
val externalBsegTable = snc.createExternalTable("bsegInGem",
   "gemfire",
   Map[String, String]("region.path" -> "bseg1", "data.as.pdx" -> "true",
   "key.class" -> "java.lang.Long"     
     ))
```

<a id= gemfirepojo> </a>
### Exposing Regions Containing POJOs as External Tables

In the following example, an external table is created using the getter methods on POJOs as TIBCO ComputeDB column names:

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
### Expose GemFire Region Using Dataframe Based External Tables

In the following example, a DataFrame is used to create an external table **bsegTable**, with the schema which is same as that of DataFrame **bsegDF**. The primary key column name should be specified for this to work correctly. In the following example, it is assumed that the dataframe contains a column named "id1".
```
bsegDF.write.format("gemfire").  
      option("region.path", "bseg1").
      option("primary.key.column.name", "id1").
      option("pdx.identity.fields", "id1").
      saveAsTable("bsegTable")
```
!!!Note
	The **pdx.identity.fields** specification has a huge impact on performance within GemFire since this specification informs GemFire to use only the specified field for computing hashCode and equality for PdxInstance.
    
<a id= gemfirerdds> </a>
### Expose GemFire Regions as RDDs
Invoking the **gemfireRegion** method on the SparkContext in TIBCO ComputeDB exposes the full data set of a GemFire region as a Spark RDD.
The same API exposes both replicated and partitioned region as RDDs.

```
scala> val rdd = sc.gemfireRegion[String, String]("gemTable1")

scala> rdd.foreach(println)
(1,one)
(3,three)
(2,two)
```

!!!Note
	when the RDD is used, it is important to specify the correct type for both the region key and value, otherwise a **ClassCastException** is encountered.

<a id= controlschemaofexternaltable> </a>
### Controlling the Schema of the External Table
The pdx fields (of pdx instances) or the getter methods (of the POJOs) define the schema of the external table by default. However, you can also control the schema by excluding the columns as per requirement. This is done by implementing the following trait:

```
package io.snappydata.spark.gemfire.connector.lifecycle
import org.apache.spark.sql.types.StructType

trait ColumnSelector {
  def selectColumns(gridName: Option[String], regionName: String,
      allColumns: StructType): StructType
}
```
Based on the requirement, a new StructType which contains fewer columns can be returned. 

!!!Note
	The new StructType returned can only have the subset of the columns that are passed. No new columns should be added.

The **ColumnSelector** implemetation needs to be added to the startup classpath of the TIBCO ComputeDB Cluster.
At the time of table creation, an option with key = **column.selector** and value as the fully qualified class name of the ColumnSelector implementation class should be passed.

### Other Optional Configuration Parameters

| column | column |
|--------|--------|
|    **max.rows.restrict**    | This parameter is used to restrict the number of rows that are fetched from an external table, when a query of type **select * from external_table** is executed. This restriction, if specified,  is applied to queries without any filter, limit, aggregate function, and projection such that it tends to bring all the rows from the external region. Note that this restriction is not applied in case of DDL statement such as **create table X using row column as select * from external_table**. The default value for is 10,000.       |
|   **column.selector**     |  This parameter is used to control the columns that must be included in the schema as described [here](#controlschemaofexternaltable).      |
|    **max.buckets.per.partition**    |   This parameter is used to control the concurrency and number of tasks created to fetch data from an external region. This property is useful only for Partitioned Region. For more details, refer to [Controlling Task Concurrency in TIBCO ComputeDB When Accessing GemFire](#controllingtaskconcurrency).  The default value is 3.  |
|    **security.username**  and **security.password**    |  By default the external table is created and queried using the credential of the current user. For example user X creates external table, and if user Y is querying, then credentials of user Y is fetched from data. But if the parameters **security.username**  and **security.password** are passed then those credentials are used to create external table and for querying the data irrespective of the current user.      |
|     **pdxtype.maxscan**   |   This parameter determines the maximum number of entries from the region which is scanned to completely determine the schema of the external table from the Pdx instances that is stored in the region. The default value is 100.     |

<a id= controllingtaskconcurrency> </a>
### Controlling Task Concurrency in TIBCO ComputeDB When Accessing GemFire 

There are two types of regions in GemFire; **replicated** and **partitioned**. All the data for a replicated region is present on every server where the region is defined. A GemFire partitioned region splits the data across the servers that define the partitioned region. 

When operating with replicated regions, there is only a single RDD partition representing the replicated region in TIBCO ComputeDB. Since there is only one data bucket for the replicated region. As compared to this, a GemFire partitioned region can be represented by a configurable number of RDD partitions in TIBCO ComputeDB. 

Choosing the number of RDD partitions directly controls the task concurrency in TIBCO ComputeDB when you run queries on GemFire regions. By default, the GemFire connector works out the number of buckets per GemFire server, assigns partitions to each server, and uses a default value of maximum three buckets per partition. You can configure the **max.buckets.per.partition** attribute to change this value. 

When queries are executed on an external table, the degree of parallelism in query execution is directly proportional to the number of RDD partitions that represents the table.

The following example shows how to configure the RDD partitions count for an external table representing a GemFire region:

```
import io.snappydata.spark.gemfire.connector._

val externalBsegTable = snc.createExternalTable("bsegInGem",
   "gemfire",
   Map[String, String]("region.path" -> "bseg1", "data.as.pdx" -> "true",
   "key.class" -> "java.lang.Long" , "max.buckets.per.partition" -> "5"    
     ))
```
## Saving Data to GemFire Region

You can save data to GemFire Regions using any of the following :

*	[Saving Pair RDD to GemFire Region](#savepairrddgemfire)
*	[Saving Non-Pair RDD to GemFire](#savenonpairrddgemfire)
*	[Saving DataFrame to GemFire](#savedataframegemfire)

<a id= savepairrddgemfire> </a>
### Saving Pair RDD to GemFire Region

A pair RDD can be saved from TIBCO ComputeDB into a GemFire region as follows: 

1.	Import the implicits as shown:

		import io.snappydata.spark.gemfire.connector
       
2.	In the Spark shell, create a simple pair RDD and save it to GemFire Region:

        scala> import io.snappydata.spark.gemfire.connector._
        scala> val data = Array(("1", "one"), ("2", "two"), ("3", "three"))
        data: Array[(String, String)] = Array((1,one), (2,two), (3,three))

        scala> val distData = sc.parallelize(data)
        distData: org.apache.spark.rdd.RDD[(String, String)] = ParallelCollectionRDD[0] at parallelize at <console>:14

        scala> distData.saveToGemFire("gemTable1")
        15/02/17 07:11:54 INFO DAGScheduler: Job 0 finished: runJob at GemFireRDDFunctions.scala:29, took 0.341288 s

3.	Verify the data is saved in GemFire using `gfsh`:

        gfsh>query --query="select key,value from /gemTable1.entries"

        Result     : true
        startCount : 0
        endCount   : 20
        Rows       : 3

        key | value
        --- | -----
        1   | one
        3   | three
        2   | two

<a id= savenonpairrddgemfire> </a>
### Saving Non-Pair RDD to GemFire 
Saving a non-pair RDD to GemFire requires an extra function that converts each element of RDD to a key-value pair. 
Here's a sample session in Spark shell:

```
scala> import io.snappydata.spark.gemfire.connector._
scala> val data2 = Array("a","ab","abc")
data2: Array[String] = Array(a, ab, abc)

scala> val distData2 = sc.parallelize(data2)
distData2: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[0] at parallelize at <console>:17

scala> distData2.saveToGemFire("gemTable1", e => (e.length, e))
[info 2015/02/17 12:43:21.174 PST <main> tid=0x1]
...
15/02/17 12:43:21 INFO DAGScheduler: Job 0 finished: runJob at GemFireRDDFunctions.scala:52, took 0.251194 s
```

Verify the result with `gfsh`:
```
gfsh>query --query="select key,value from /gemTable1.entrySet"

Result     : true
startCount : 0
endCount   : 20
Rows       : 3

key | value
--- | -----
2   | ab
3   | abc
1   | a
```

<a id= savedataframegemfire> </a>
### Saving a DataFrame to GemFire
To save a DataFrame, that is dataSet of row objects, into GemFire, use the following API which is available as an implicit definition. The rows of the dataframes are converted into PDX instances for storage in the GemFire's region.

In the following example, it is assumed that there is a column "id1" present in the dataframe's schema. To specify the PDX Identity Fields for the PDX Type, use the option as ("pdx.identity.fields", "Col1, Col2, Col3") to specify one or more columns to be used as PDX Identity fields. 
TIBCO ComputeDB recommends to define the identity fields for performance during comparison of PDX Instances.

```
import io.snappydata.spark.gemfire.connector._

df.write.format("gemfire").
          option("region.path","/region1").
          option("primary.key.column.name", "id1").
          option("pdx.identity.fields", "id1")
          .save()
```      

To dynamically generate the GemFire Region's key, import the implicits and use the following API:

```
saveToGemFire[K](regionPath: String, keyExtractor: Row => K, opConf: Map[String, String] = Map.empty )
```

```
import io.snappydata.spark.gemfire.connector._

df.saveToGemFire[String]("/region1", 
(row: Row) => ( row.getString(1) + "_" + row.getString(10)),
Map[String, String]("pdx.identity.fields" -> "id1, id10")
)
```    

<a id= runoqlqueriesgemfire> </a>
## Running OQL queries Directly on GemFire from TIBCO ComputeDB

Most applications using TIBCO ComputeDB will choose to run regular SQL queries on GemFire regions. Refer to [Accessing Data From GemFire](#accessingdatafromgemfire) 
Additionally, you can directly execute OQL queries on GemFire regions using the GemFire connector. In scenarios where the data stored in GemFire regions is neither PDX nor Java bean compliant POJO, you can execute OQL queries and retrieve the data from the server and make it available as a data frame.

An instance of `SQLContext` is required to run OQL query.

```
val snc = new org.apache.spark.sql.SnappyContext(sc)
```

Create a `DataFrame` using OQL:
```
val dataFrame = snc.gemfireOQL("SELECT iter.name,itername.address.city, iter.id FROM /personRegion iter")
```

You can repartition the `DataFrame` using `DataFrame.repartition()` if required. 
After you have the `DataFrame`, you can register it as a table and use Spark 
SQL to query:

```
dataFrame.registerTempTable("person")
val SQLResult = sqlContext.sql("SELECT * FROM person WHERE id > 100")
```

