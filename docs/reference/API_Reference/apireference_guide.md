# TIBCO ComputeDB Spark Extension API Reference Guide

This guide gives details of Spark extension APIs that are provided by TIBCO ComputeDB. The following APIs are included:


| SnappySession APIs | DataFrameWriter APIs |SnappySessionCatalog APIs|
|--------|--------|--------|
|  [**sql**](#sqlapi)   </br> Query Using Cached Plan   |  [**putInto**](#putintoapi)</br>Put Dataframe Content into Table  | [**getKeyColumns**](#getkeycolumapi) </br>Get Key Columns of TIBCO ComputeDB table|
|  [**sqlUncached**](#sqluncachedapi)</br>Query Using Fresh Plan   | [**deleteFrom**](#deletefromapi)</br>Delete DataFrame Content from Table |[**getKeyColumnsAndPositions**](#getkeycolumnspos) </br>Gets primary key or key columns with their position in the table. |
|   [**createTable**](#createtableapi)</br>Create TIBCO ComputeDB Managed Table    |        ||
|     [**createTable**](#createtable1)</br>Create TIBCO ComputeDB Managed JDBC Table |        ||
|    [**truncateTable**](#truncateapi)</br> Empty Contents of Table    |        ||
|    [**dropTable**](#droptableapi) </br>Drop TIBCO ComputeDB Table    |        ||
|  [**createSampleTable**](#createsampletableapi)</br>Create Stratified Sample Table      |        ||
|   [**createApproxTSTopK**](#createaproxtstopkapi)</br>Create Structure to Query Top-K     |        ||
|    [**setSchema**](#setschemaapi)</br>Set Current Database/schema    |        ||
|   [**getCurrentSchema**](#getcurrentschemaapi)</br>Get Current Schema of Session     |        ||
|    [**insert**](#insertapi)</br>Insert Row into an Existing Table   |        ||
|   [**put**](#putapi)</br>Upsert Row into an Existing Table    |        ||
|   [**update**](#updatedapi)</br>Update all Rows in Table  |        ||
|     [**delete**](#deleteapi)</br>Delete all Rows in Table  |        ||
|     [**queryApproxTSTopK**](#queryapproxtstapi)</br>Fetch the TopK Entries  |        ||

<a id= snappysessionscala> </a>
## SnappySession APIs
The following APIs are available for SnappySession.

*	[**sql**](#sqlapi)
*	[**sqlUncached**](#sqluncachedapi)
*	[**createTable**](#createtableapi)
*	[**truncateTable**](#truncateapi)
*	[**dropTable**](#droptableapi)
*	[**createSampleTable**](#createsampletableapi)
*	[**createApproxTSTopK**](#createaproxtstopkapi)
*	[**setSchema**](#setschemaapi)
*	[**getCurrentSchema**](#getcurrentschemaapi)
*	[**insert**](#insertapi)
*	[**put**](#putapi)
*	[**delete**](#deleteapi)
*	[**queryApproxTSTopK**](#queryapproxtstapi)

<a id= sqlapi> </a>
### sql
You can use this API to run a query with a cached plan for a given SQL.

**Syntax**

```
sql(sqlText : String)
```


**Parameters**

|Parameter	 | Description |
|--------|--------|
| sqlText | The SQL string required to execute.  |
|Returns |Dataframe|


**Example** 

```
snappySession.sql(“select * from t1”)
```

<a id= sqluncachedapi> </a>
### sqlUncached

You can use this API to run a query using a fresh plan for a given SQL String.

**Syntax**

```
sqlUncached(sqlText : String)
```

**Parameters**

|Parameter	 | Description |
|--------|--------|
| sqlText  |The SQL string required to execute.|
|Returns |Dataframe|

**Example **

```pre
snappySession.sqlUncached(“select * from t1”)
```
<a id= createtableapi> </a>
### createTable

Creates a TIBCO ComputeDB managed table. Any relation providers, that is the row, column etc., which are supported by TIBCO ComputeDB can be created here.

**Syntax**

```
createTable(
      tableName: String,
      provider: String,
      schemaDDL: String,
      options: Map[String, String],
      allowExisting: Boolean)
```

**Parameters**

|Parameter	 | Description |
|--------|--------|
|  tableName |  Name of the table.    | 
|provider  |Provider name such as ‘ROW’, ‘COLUMN’' etc.|
|schema   | The table schema.|
|  options | Properties for table creation. For example, partition_by, buckets etc.|
| allowExisting |When set to **true**, tables with the same name are ignored, else an **AnalysisException** is thrown stating that the table already exists. |
|Returns |Dataframe |

**Example**

```
case class Data(col1: Int, col2: Int, col3: Int)
val props = Map.empty[String, String]
val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
val dataDF = snappySession.createDataFrame(rdd)

snappySession.createTable(tableName, "column", dataDF.schema, props)

```
<a id= createtable1> </a>
### createTable

Creates a TIBCO ComputeDB managed JDBC table which takes a free format DDL string. The DDL string should adhere to the syntax of the underlying JDBC store. TIBCO ComputeDB ships with an inbuilt JDBC store, which can be accessed by the data store of Row format. The options parameter can take connection details.

**Syntax**

```
Syntax: 
  createTable(
      tableName: String,
      provider: String,
      schemaDDL: String,
      options: Map[String, String],
      allowExisting: Boolean)

```

**Parameters**

|Parameter	 | Description |
|--------|--------|
|  tableName |  Name of the table. | 
|provider  |Provider name such as ‘ROW’, ‘COLUMN’' etc.|
|schemaDDL   |The table schema as a string interpreted by the provider.|
|  options | Properties for table creation. For example, partition_by, buckets etc.|
|allowExisting ||When set to **true**, tables with the same name are ignored, else an **AnalysisException** is thrown stating that the table already exists.|

**Example**

```
   val props = Map(
      "url" -> s"jdbc:derby:$path",
      "driver" -> "org.apache.derby.jdbc.EmbeddedDriver",
      "poolImpl" -> "tomcat",
      "user" -> "app",
       "password" -> "app"
       )
   
    val schemaDDL = "(OrderId INT NOT NULL PRIMARY KEY,ItemId INT, ITEMREF INT)"
    snappySession.createTable("jdbcTable", "jdbc", schemaDDL, props)

```
<a id= truncateapi> </a>
### truncateTable

Empties the contents of the table without deleting the catalog entry.


**Syntax**

```
truncateTable(tableName: String, ifExists: Boolean = false)

```

**Parameters**

|Parameter	 | Description |
|--------|--------|
|     tableName   |       Name of the table.  | 
|ifExists |Attempt truncate only if the table exists.|
|Returns|Dataframe|

**Example **

```pre
snappySession.truncateTable(“t1”, true)
```
<a id= droptableapi> </a>
### dropTable

Drop a TIBCO ComputeDB table created by a call to **SnappySession.createTable**, **Catalog.createExternalTable** or **Dataset.createOrReplaceTempView**. 

**Syntax**

```
dropTable(tableName: String, ifExists: Boolean = false)

```

**Parameters**

|Parameter	 | Description |
|--------|--------|
| tableName       |     Name of the table.    | 
|ifExists |Attempts drop only if the table exists.|
|Returns|Unit|

**Example **

```pre
snappySession.dropTable(“t1”, true)
```

<a id= createsampletableapi> </a>
### createSampleTable

Creates a stratified sample table.

!!! Note
	This API is not supported in the Smart Connector mode. 
 

**Syntax**

```
createSampleTable(tableName: String,
      baseTable: Option[String],
      samplingOptions: Map[String, String],
      allowExisting: Boolean)


```

**Parameters**

|Parameter	 | Description |
|--------|--------|
| tableName       |   The qualified name of the table. | 
|baseTable |The base table of the sample table, if any.|
| samplingOptions |sampling options such as QCS, reservoir size etc.|
|allowExisting|When set to **true**,  tables with the same name are ignored, else a **table exist** exception is shown.|
|Returns |Dataframe |


**Example **

```pre
snappySession.createSampleTable("airline_sample",   Some("airline"), Map("qcs" -> "UniqueCarrier ,Year_ ,Month_",  "fraction" -> "0.05",  "strataReservoirSize" -> "25", "buckets" -> "57"),
 allowExisting = false)
```
<a id= createaproxtstopkapi> </a>
### createApproxTSTopK

Creates an approximate structure to query top-K with time series support.

!!! Note
	This API is not supported in the Smart Connector mode. 

**Syntax**

```
createApproxTSTopK(topKName: String, baseTable: Option[String],  keyColumnName: String, inputDataSchema: StructType,       topkOptions: Map[String, String], allowExisting: Boolean = false)

```

**Parameters**

|Parameter	 | Description |
|--------|--------|
| topKName       | The qualified name of the top-K structure. | 
|baseTable |The base table of the top-K structure, if any.|
| keyColumnName ||
|inputDataSchema| |
|topkOptions| |
|allowExisting |When set to **true**,  tables with the same name are ignored, else a **table exist** exception is shown.|
|Returns |Dataframe |


**Example **

```pre
snappySession.createApproxTSTopK("topktable", Some("hashtagTable"), "hashtag", schema, topKOption)
```

<a id= setschemaapi> </a>
### setSchema

Sets the current database/schema.

**Syntax**

```
setSchema(schemaName: String)

```

**Parameters**

|Parameter	 | Description |
|--------|--------|
| schemaName| schema name which goes into the catalog. | 
|Returns|Unit|

**Example **

```pre
snappySession.setSchema(“APP”)
```

<a id= getcurrentschemaapi> </a>
### getCurrentSchema

Gets the current schema of the session.

**Syntax**

```
getCurrentSchema

```

**Example **

```pre
snappySession.getCurrentSchema
```

**Returns**

String

<a id= insertapi> </a>
### insert

Inserts one or more row into an existing table.

**Syntax**

```
insert(tableName: String, rows: Row*)


```

**Parameters**

|Parameter	 | Description |
|--------|--------|
| tableName| Table name for the insert operation.|
|Rows|List of rows to be inserted into the table.|
|Returns|Int|

**Example **

```pre
val row = Row(i, i, i)
snappySession.insert("t1", row)

```
<a id= putapi> </a>
### put

Upserts one or more row into an existing table. Only works for row tables.


**Syntax**

```
put(tableName: String, rows: Row*)
```

**Parameters**

|Parameter	 | Description |
|--------|--------|
| tableName       | Table name for the put operation | 
|rows| List of rows to be put into the table.|
|Returns|Int|

**Example **

```pre
snappySession.put(tableName, dataDF.collect(): _*)
```

<a id= updatedapi> </a>
### update

Updates all the rows in the table that match passed filter expression. This works only for row tables.

**Syntax**

```
update(tableName: String, filterExpr: String, newColumnValues: Row,  updateColumns: String*)
```

**Parameters**

|Parameter	 | Description |
|--------|--------|
| tableName    |   Th table name which needs to be updated.|
|filterExpr| SQL WHERE criteria to select rows that will be updated.| 
|newColumnValues| A single row containing all the updated column values. They MUST match the **updateColumn: list passed**.|
|updateColumns| List of all column names that are updated.|
|Returns|Int|


**Example **

```pre
snappySession.update("t1", "ITEMREF = 3" , Row(99) , "ITEMREF" )
```

<a id= deleteapi> </a>
### delete

Deletes all the rows in the table that match passed filter expression. This works only for row tables.


**Syntax**

```
delete(tableName: String, filterExpr: String)
```

**Parameters**

|Parameter	 | Description |
|--------|--------|
| tableName      | Name of the table. |
|filterExpr|  SSQL WHERE criteria to select rows that will be updated. | 
|Returns|Int|

**Example **

```pre
snappySession.delete(“t1”, s"col1=$i"))
```

<a id= queryapproxtstapi> </a>
### queryApproxTSTopK

Fetches the topK entries in the** Approx TopK** synopsis for the specified time interval. The time interval specified here should not be less than the minimum time interval used when creating the TopK synopsis.


!!! Note
	This API is not supported in the Smart Connector mode. 

**Syntax**

```
queryApproxTSTopK(topKName: String,
      startTime: String = null, endTime: String = null,
      k: Int = -1)

```

**Parameters**

|Parameter	 | Description |
|--------|--------|
| topKName      |   The topK structure that is to be queried.|
|startTime|  Start time as string in the format **yyyy-mm-dd hh:mm:ss**.  If passed as **null**, the oldest interval is considered as the start interval.| 
|endTime| End time as string in the format **yyyy-mm-dd hh:mm:ss**. If passed as **null**, the newest interval is considered as the last interval.|
|k| Optional. The number of elements to be queried. This is to be passed only for stream summary|
|Returns|Dataframe|



**Example **

```pre
snappySession.queryApproxTSTopK("topktable")
```

<a id= dataframewriter> </a>
## DataFrameWriter APIs
The following APIs are available for DataFrameWriter:

*	[**putInto**](#putintoapi)
*	[**deleteFrom**](#deletefromapi)

<a id= putintoapi> </a>
### putInto

Puts the content of the DataFrame into the specified table. It requires that the schema of the DataFrame is the same as the schema of the table. Column names are ignored while matching the schemas and **put into** operation is performed using position based resolution.
If some rows are already present in the table, then they are updated. Also, the table on which **putInto** is implemented should have defined key columns, if its a column table. If it is a row table, then it should have defined a primary key.

**Syntax**

```
putInto(tableName: String)

```

**Parameters**

|Parameter	 | Description |
|--------|--------|
| tableName      |Name of the table.|
|Returns|Unit|


**Example **

```pre
import org.apache.spark.sql.snappy._
df.write.putInto(“snappy_table”)
```
<a id= deletefromapi> </a>
### deleteFrom
The `deleteFrom` API deletes all those records from given Snappy table which exists in the input Dataframe. Existence of the record is checked by comparing the key columns (or the primary keys) values. 

To use this API, key columns(for column table) or primary keys(for row tables) must be defined in the TIBCO ComputeDB table.

Also, the source DataFrame must contain all the key columns or primary keys (depending upon the type of Snappy table). The column existence is checked using a case-insensitive match of column names. If the source DataFrame contains columns other than the key columns, it will be ignored by the `deleteFrom` API.


**Syntax**

```
deleteFrom(tableName: String)
```

**Parameters**

|Parameter	 | Description |
|--------|--------|
| tableName      |    Name of the table.|
|Returns|Unit|

**Example **

```pre
import org.apache.spark.sql.snappy._
df.write.deleteFrom(“snappy_table”)

```

## SnappySessionCatalog APIs
The following APIs are available for SnappySessionCatalog:

*	[**getKeyColumns**](#getkeycolumapi)
*	[**getKeyColumnsAndPositions**](#getkeycolumnspos) 

!!! Note
	These are developer APIs and are subject to change in the future.

<a id= getkeycolumapi> </a>
### getKeyColumns
Gets primary key or key columns of a TIBCO ComputeDB table.

**Syntax**

```
getKeyColumns(tableName: String)

```

**Parameters**

|Parameter	 | Description |
|--------|--------|
| tableName      |    Name of the table.|
| Returns     |    Sequence of key columns (for column tables) or sequence of primary keys (for row tables).|
**Example **

```pre
snappySession.sessionCatalog.getKeyColumns("t1")
```

### getKeyColumnsAndPositions
<a id= getkeycolumnspos> </a>

Gets primary key or key columns of a SnappyData table along with their position in the table.

**Syntax**

```
getKeyColumnsAndPositions(tableName: String)
```

**Parameters**

|Parameter	 | Description |
|--------|--------|
| tableName	    |    Name of the table.|
| Returns     |  Sequence of `scala.Tuple2` containing column name and column's position in the table for each key columns (for column tables) or sequence of primary keys (for row tables).|

**Example **

```pre
snappySession.sessionCatalog.getKeyColumnsAndPositions("t1")

```
