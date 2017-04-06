# SNAPPYSESSION
<!-- def -->
## appendToTempTableCache
** Function**: appendToTempTableCache(df: DataFrame, table: String, storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK): Unit

**Description**: Append dataframe to cache table in Spark.
    
**Parameters**:  

| Parameter | Description |
|--------|--------|
|df  |        |
|table        |        |
|storageLevel        |        |

    
**Notes**:  

<!-- def -->
## baseRelationToDataFrame
** Function**: baseRelationToDataFrame**(baseRelation: BaseRelation): DataFrame

**Description** Convert a BaseRelation created for external data sources into a DataFrame.
    
**Parameters**: 

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |
    
**Notes**:  

<!-- lazy val  -->
## catalog
**Function**: catalog: Catalog

**Description**: Interface through which the user may create, drop, alter or query underlying databases, tables, functions etc.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## clear
**Function**: clear(): Unit

**Description**: 
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes**:

<!-- def -->
## clearPlanCache
**Function**: clearPlanCache(): Unit

**Description**: 
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |
   
**Notes:**  

<!-- lazy val  -->
## conf

**Function**: conf: RuntimeConfig

**Description**: Runtime configuration interface for Spark.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |
    
**Notes:**  

<!-- def -->
## createApproxTSTopK
**Function**: createApproxTSTopK(topKName: String, baseTable: String, keyColumnName: String, topkOptions: Map[String, String], allowExisting: Boolean): DataFrame

**Description**: Create approximate structure to query top-K with time series support.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |
    
**Notes:**  

<!-- def -->
## createApproxTSTopK
**Function**: createApproxTSTopK(topKName: String, baseTable: Option[String], keyColumnName: String, topkOptions: Map[String, String], allowExisting: Boolean): DataFrame

**Description**: Create approximate structure to query top-K with time series support.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## createApproxTSTopK
**Function**: createApproxTSTopK(topKName: String, baseTable: String, keyColumnName: String, inputDataSchema: StructType, topkOptions: Map[String, String], allowExisting: Boolean): DataFrame

**Description**: Create approximate structure to query top-K with time series support.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |
    
**Notes:**  

<!-- def -->
## createApproxTSTopK
**Function**: createApproxTSTopK(topKName: String, baseTable: Option[String], keyColumnName: String, inputDataSchema: StructType, topkOptions: Map[String, String], allowExisting: Boolean = false): DataFrame

**Description**: Create approximate structure to query top-K with time series support.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## createDataFrame
**Function**: createDataFrame(data: List[_], beanClass: Class[_]): DataFrame

**Description**: Applies a schema to a List of Java Beans.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## createDataFrame
**Function**: createDataFrame(rdd: JavaRDD[_], beanClass: Class[_]): DataFrame

**Description**: Applies a schema to an RDD of Java Beans.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## createDataFrame
**Function**: createDataFrame(rdd: RDD[_], beanClass: Class[_]): DataFrame

**Description**: Applies a schema to an RDD of Java Beans.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## createDataFrame
**Function**: createDataFrame(rows: List[Row], schema: StructType): DataFrame

**Description**: **Description**: :: DeveloperApi :: Creates a DataFrame from a java.util.List containing Rows using the given schema.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## createDataFrame
**Function**: createDataFrame(rowRDD: JavaRDD[Row], schema: StructType): DataFrame

**Description**: **Description**: :: DeveloperApi :: Creates a DataFrame from a JavaRDD containing Rows using the given schema.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## createDataFrame
**Function**: createDataFrame(rowRDD: RDD[Row], schema: StructType): DataFrame

**Description**: **Description**: :: DeveloperApi :: Creates a DataFrame from an RDD containing Rows using the given schema.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## createDataFrame
**Function**: createDataFrame[A <: Product](data: Seq[A])(implicit arg0: scala.reflect.api.JavaUniverse.TypeTag[A]): DataFrame

**Description**: :: Experimental :: Creates a DataFrame from a local Seq of Product.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## createDataFrame
**Function**: createDataFrame[A <: Product](rdd: RDD[A])(implicit arg0: scala.reflect.api.JavaUniverse.TypeTag[A]): DataFrame

**Description**: :: Experimental :: Creates a DataFrame from an RDD of Product (e.g.)
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## createDataset
**Function**: createDataset[T](data: RDD[T])(implicit arg0: Encoder[T]): Dataset[T]

**Description**: :: Experimental :: Creates a Dataset from an RDD of a given type.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |
       
**Notes:**  

<!-- def -->
## createDataset
**Function**: createDataset[T](data: List[T])(implicit arg0: Encoder[T]): Dataset[T]

**Description**: :: Experimental :: Creates a Dataset from a java.util.List of a given type.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## createDataset
**Function**: createDataset[T](data: Seq[T])(implicit arg0: Encoder[T]): Dataset[T]

**Description**: :: Experimental :: Creates a Dataset from a local Seq of data of a given type.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## createIndex
**Function**: createIndex(indexName: String, baseTable: String, indexColumns: Map[String, Option[SortDirection]], options: Map[String, String]): Unit

**Description**:Create an index on a table.

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |
    
**Notes:**  

<!-- def -->
## createIndex
**Function**: createIndex(indexName: String, baseTable: String, indexColumns: Map[String, Boolean], options: Map[String, String]): Unit

**Description**: Create an index on a table.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## createSampleTable
**Function**: createSampleTable(tableName: String, baseTable: String, schema: StructType, samplingOptions: Map[String, String], allowExisting: Boolean): DataFrame

**Description**: Create a stratified sample table.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## createSampleTable
**Function**: createSampleTable(tableName: String, baseTable: Option[String], schema: StructType, samplingOptions: Map[String, String], allowExisting: Boolean = false): DataFrame

**Description**: Create a stratified sample table.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## createSampleTable
**Function**: createSampleTable(tableName: String, baseTable: String, samplingOptions: Map[String, String], allowExisting: Boolean): DataFrame

**Description**: Create a stratified sample table.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## createSampleTable
**Function**: createSampleTable(tableName: String, baseTable: Option[String], samplingOptions: Map[String, String], allowExisting: Boolean): DataFrame

**Description**: Create a stratified sample table.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## createTable
**Function**: createTable(tableName: String, provider: String, schemaDDL: String, options: Map[String, String], allowExisting: Boolean): DataFrame

**Description**: Creates a SnappyData managed JDBC table which takes a free format ddl string.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## createTable
**Function**: createTable(tableName: String, provider: String, schemaDDL: String, options: Map[String, String], allowExisting: Boolean): DataFrame

**Description**: Creates a SnappyData managed JDBC table which takes a free format ddl string.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## createTable
**Function**: createTable(tableName: String, provider: String, schema: StructType, options: Map[String, String], allowExisting: Boolean): DataFrame

**Description**: Creates a SnappyData managed table.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## createTable
**Function**: createTable(tableName: String, provider: String, schema: StructType, options: Map[String, String], allowExisting: Boolean = false): DataFrame

**Description**: Creates a SnappyData managed table.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## createTable
**Function**: createTable(tableName: String, provider: String, options: Map[String, String], allowExisting: Boolean): DataFrame

**Description**: Creates a SnappyData managed table.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## createTable
**Function**: createTable(tableName: String, provider: String, options: Map[String, String], allowExisting: Boolean): DataFrame

**Description**: Creates a SnappyData managed table.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## delete
**Function**: delete(tableName: String, filterExpr: String): Int

**Description**: Delete all rows in table that match passed filter expression
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |
    
**Notes:**  

<!-- def -->
## dropIndex
**Function**: dropIndex(indexName: QualifiedTableName, ifExists: Boolean): Unit

**Description**: Drops an index on a table
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## dropIndex
**Function**: dropIndex(indexName: String, ifExists: Boolean): Unit

**Description**: Drops an index on a table
    
**Parameters**:
    
| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

**Notes:**  

<!-- def -->
## dropTable
**Function**: dropTable(tableName: String, ifExists: Boolean = false): Unit

**Description**: Drop a SnappyData table created by a call to SnappyContext.createTable, createExternalTable or registerTempTable.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- lazy val  -->
## emptyDataFrame
**Function**: emptyDataFrame: DataFrame

**Description**: Returns a DataFrame with no rows or columns.
    
**Parameters**:


| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## emptyDataset
**Function**: emptyDataset[T](implicit arg0: Encoder[T]): Dataset[T]

**Description**: :: Experimental :: Creates a new Dataset of type T containing zero elements.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## experimental
**Function**: experimental: ExperimentalMethods

**Description**: :: Experimental :: A collection of methods that are considered experimental, but can be used to hook into the query planner for advanced functionality.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## getClass
**Function**: getClass(ctx: CodegenContext, baseTypes: Seq[(DataType, Boolean)], keyTypes: Seq[(DataType, Boolean)], types: Seq[(DataType, Boolean)]): Option[(String, String)]

**Description**: Get name of a previously registered class using addClass.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## getExCode
**Function**: getExCode(ctx: CodegenContext, vars: Seq[String], expr: Seq[Expression]): Option[ExprCodeEx]
 
**Description**: Get ExprCodeEx for a previously registered ExprCode variable using addExCode.

**Parameters**:


| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## getPreviousQueryHints
**Function**: getPreviousQueryHints: Map[String, String]

**Description**: 
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- object -->
## implicits
**Function**: implicits extends SQLImplicits with Serializable

**Description**: :: Experimental :: (Scala-specific) Implicit methods available in Scala for converting common Scala objects into DataFrames.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## insert
**Function**: insert(tableName: String, rows: ArrayList[ArrayList[_]]): Int

**Description**: Insert one or more org.apache.spark.sql.Row into an existing table A user can insert a DataFrame using foreachPartition...
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## insert
**Function**: insert(tableName: String, rows: Row*): Int
* 
**Description**: Insert one or more org.apache.spark.sql.Row into an existing table A user can insert a DataFrame using foreachPartition...
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## listenerManager
**Function**: listenerManager: ExecutionListenerManager

**Description**: :: Experimental :: An interface to register custom org.apache.spark.sql.util.QueryExecutionListeners that listen for execution metrics.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## newSession
**Function**: newSession(): SnappySession

**Description**: Start a new session with isolated SQL configurations, temporary tables, registered functions are isolated, but sharing the underlying SparkContext and cached data.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## put
**Function**: put(tableName: String, rows: ArrayList[ArrayList[_]]): Int

**Description**: Upsert one or more org.apache.spark.sql.Row into an existing table upsert a DataFrame using foreachPartition...
    
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## put
**Function**: put(tableName: String, rows: Row*): Int

**Description**: Upsert one or more org.apache.spark.sql.Row into an existing table upsert a DataFrame using foreachPartition...
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## queryApproxTSTopK
**Function**: queryApproxTSTopK(topK: String, startTime: Long, endTime: Long, k: Int): DataFrame

<!-- def -->
## queryApproxTSTopK
**Function**: queryApproxTSTopK(topKName: String, startTime: Long, endTime: Long): DataFrame

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## queryApproxTSTopK
**Function**: queryApproxTSTopK(topKName: String, startTime: String = null, endTime: String = null, k: Int = 1): DataFrame

**Description**: Fetch the topK entries in the Approx TopK synopsis for the specified time interval.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## range
**Function**: range(start: Long, end: Long, step: Long, numPartitions: Int): Dataset[Long]

**Description**: :: Experimental :: Creates a Dataset with a single LongType column named id, containing elements in a range from start to end (exclusive) with a step value, with partition number specified.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |
    
**Notes:**  

<!-- def -->
## range
**Function**: range(start: Long, end: Long, step: Long): Dataset[Long]

**Description**: :: Experimental :: Creates a Dataset with a single LongType column named id, containing elements in a range from start to end (exclusive) with a step value.

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |
    
**Notes:**  

<!-- def -->
## range
**Function**: range(start: Long, end: Long): Dataset[Long]

**Description**: :: Experimental :: Creates a Dataset with a single LongType column named id, containing elements in a range from start to end (exclusive) with step value 1.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## range
**Function**: range(end: Long): Dataset[Long]

**Description**: :: Experimental :: Creates a Dataset with a single LongType column named id, containing elements in a range from 0 to end (exclusive) with step value 1.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## read
**Function**: read: DataFrameReader

**Description**: Returns a DataFrameReader that can be used to read non-streaming data in as a DataFrame.

<!-- def -->
## readStream
**Function**: readStream: DataStreamReader

**Description**: :: Experimental :: Returns a DataStreamReader that can be used to read streaming data in as a DataFrame.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## saveStream
**Function**: saveStream[T](stream: DStream[T], aqpTables: Seq[String], transformer: Option[(RDD[T]) ⇒ RDD[Row]])(implicit v: scala.reflect.api.JavaUniverse.TypeTag[T]): Unit

**Description**: :: DeveloperApi ::
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- lazy val  -->
## sessionCatalog
**Function**: sessionCatalog: SnappyStoreHiveCatalog

**Description**: 
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## setSchema
**Function**: setSchema(schemaName: String): Unit
Set current database/schema.

<!-- val -->
## sparkContext
**Function**: sparkContext: SparkContext

**Description**: 
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## sql
**Function**: sql(sqlText: String): CachedDataFrame

**Description**: Executes a SQL query using Spark, returning the result as a DataFrame.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- val -->
## sqlContext
**Function**: sqlContext: SnappyContext

**Description**: A wrapped version of this session in the form of a SQLContext, for backward compatibility.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def-->
## sqlUncached
**Function**: sqlUncached(sqlText: String): DataFrame

**Description**:Stop the underlying SparkContext.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## stop
**Function**: stop(): Unit
 
**Description**:Stop the underlying SparkContext.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## streams
**Function**: streams: StreamingQueryManager
**Description**: :: Experimental :: Returns a StreamingQueryManager that allows managing all the StreamingQueries active on this.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## table
**Function**: table(tableName: String): DataFrame

**Description**: Returns the specified table as a DataFrame.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## truncateTable
**Function**: truncateTable(tableName: String, ifExists: Boolean = false): Unit

**Description**: Empties the contents of the table without deleting the catalog entry.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## udf
**Function**: udf: UDFRegistration

**Description**: A collection of methods for registering user-defined functions (UDF).
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## update
**Function**: update(tableName: String, filterExpr: String, newColumnValues: ArrayList[_], updateColumns: ArrayList[String]): Int

**Description**: Update all rows in table that match passed filter expression
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

    
**Notes:**  

<!-- def -->
## update
**Function**: update(tableName: String, filterExpr: String, newColumnValues: Row, updateColumns: String*): Int

**Description**: Update all rows in table that match passed filter expression
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

 
**Notes:**  

<!-- def -->
## version
**Function**: version: String

**Description**: The version of Spark on which this application is running.
    
**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |


**Notes:**  
