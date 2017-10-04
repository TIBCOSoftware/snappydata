<a id="markdown_link_row_and_column_tables"></a>
# Tables in SnappyData
## Row and Column Tables
Column tables organize and manage data in memory in a compressed columnar form such that, modern day CPUs can traverse and run computations like a sum or an average really fast (as the values are available in contiguous memory). Column table follows the Spark DataSource access model.

![Column Table](../Images/column_table.png)

Row tables, unlike column tables, are laid out one row at a time in contiguous memory. Rows are typically accessed using keys and its location is determined by a hash function and hence is fast for point lookups or updates.
![Column Table](../Images/row_table.png)

Create table DDL for Row and Column tables allows tables to be partitioned on primary keys, custom partitioned, replicated, carry indexes in memory, persist to disk, overflow to disk, be replicated for HA, etc.

### DDL and DML Syntax for Tables
```scala
CREATE TABLE [IF NOT EXISTS] table_name
   (
  COLUMN_DEFINITION
   )
USING row | column
OPTIONS (
COLOCATE_WITH 'table_name',  // Default none
PARTITION_BY 'PRIMARY KEY' | 'column name', // If not specified, a replicated table is created
BUCKETS  'NumPartitions', // Default 128
REDUNDANCY '1' ,
EVICTION_BY 'LRUMEMSIZE 200' | 'LRUCOUNT 200' | 'LRUHEAPPERCENT',
OVERFLOW 'true',
PERSISTENCE  'ASYNCHRONOUS' | 'ASYNC' | 'SYNCHRONOUS' | 'SYNC' | 'NONE',
DISKSTORE 'DISKSTORE_NAME', //empty string maps to default diskstore
EXPIRE 'TIMETOLIVE_in_seconds',
COLUMN_BATCH_SIZE '32000000',
COLUMN_MAX_DELTA_ROWS '10000',
)
[AS select_statement];

DROP TABLE [IF EXISTS] table_name
```

Refer to the [How-Tos](../howto.md) section for more information on partitioning and colocating data and [CREATE TABLE](../reference/sql_reference/create-table.md) for information on creating a row/column table.

You can also define complex types (Map, Array and StructType) as columns for column tables.

```
snappy.sql("CREATE TABLE tableName (
col1 INT , 
col2 Array<Decimal>, 
col3 Map<Timestamp, Struct<x: Int, y: String, z: Decimal(10,5)>>, 
col6 Struct<a: Int, b: String, c: Decimal(10,5)>
) USING column options(BUCKETS '5')" )
```

To access the complex data from JDBC you can see [JDBCWithComplexTypes](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/JDBCWithComplexTypes.scala) for examples.

!!! Note 
	Clauses like PRIMARY KEY, NOT NULL etc. are not supported for column definition.

### Spark API for Managing Tables

**Get a reference to [SnappySession](http://snappydatainc.github.io/snappydata/apidocs/#org.apache.spark.sql.SnappySession):**

    val snappy: SnappySession = new SnappySession(spark.sparkContext)

Create a SnappyStore table using Spark APIs

```scala
    val props = Map('BUCKETS','5') //This map should contain required DDL extensions, see next section
    case class Data(col1: Int, col2: Int, col3: Int)
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sparkContext.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snappy.createDataFrame(rdd)
    snappy.createTable("column_table", "column", dataDF.schema, props)
    //or create a row format table
    snappy.createTable("row_table", "row", dataDF.schema, props)
```
**Drop a SnappyStore table using Spark APIs**:

    snappy.dropTable(tableName, ifExists = true)
    
<a id="ddl"></a>
### DDL extensions to SnappyStore Tables
The below mentioned DDL extensions are required to configure a table based on user requirements. One can specify one or more options to create the kind of table one wants. If no option is specified, default values are attached. See next section for various restrictions. 

   * <b>COLOCATE_WITH:</b> The COLOCATE_WITH clause specifies a partitioned table with which the new partitioned table must be colocated. The referenced table must already exist.

   * <b>PARTITION_BY:</b> Use the PARTITION_BY {COLUMN} clause to provide a set of column names that determine the partitioning. If not specified, it is a replicated table.</br> Column and row tables support hash partitioning on one or more columns. These are specified as comma-separated column names in the PARTITION_BY option of the CREATE TABLE DDL or createTable API. The hashing scheme follows the Spark Catalyst Hash Partitioning to minimize shuffles in joins. If no PARTITION_BY option is specified for a column table, then, the table is still partitioned internally on a generated scheme.</br> The default number of storage partitions (BUCKETS) is 128 in cluster mode and two times the number of cores in local mode. This can be changed using the BUCKETS option in CREATE TABLE DDL or createTable API.

   * <b>BUCKETS:</b>  The optional BUCKETS attribute specifies the fixed number of "buckets," the smallest unit of data containment for the table that can be moved around. Data in a single bucket resides and moves together. If not specified, the number of buckets defaults to 128.

   * <b>REDUNDANCY:</b>  Use the REDUNDANCY clause to specify the number of redundant copies that should be maintained for each partition, to ensure that the partitioned table is highly available even if members fail.

   * <b>EVICTION_BY:</b>  Use the EVICTION_BY clause to evict rows automatically from the in-memory table, based on different criteria. </br>For column tables, the default eviction setting is LRUHEAPPERCENT and the default action is to overflow to disk. You can also specify the OVERFLOW parameter along with the EVICTION_BY clause.

	!!! Note:
 		For column tables, you cannot use the LRUMEMSIZE or LRUCOUNT eviction settings. For row tables, no such defaults are set. Row tables allow all the eviction settings.

   * <b>OVERFLOW:</b>  If it is set to **false** the evicted rows are destroyed. If set to **true** it overflows to a local SnappyStore disk store.
	When you configure an overflow table, only the evicted rows are written to disk. If you restart or shut down a member that hosts the overflow table, the table data that was in memory is not restored unless you explicitly configure persistence (or you configure one or more replicas with a partitioned table).

   * <b>PERSISTENCE:</b>  When you specify the PERSISTENCE keyword, SnappyData persists the in-memory table data to a local SnappyData disk store configuration. SnappyStore automatically restores the persisted table data to memory when you restart the member.
   	
!!! Note:
   	* By default, both row and column tables are persistent.

   	* The option `PERSISTENT` has been deprecated as of SnappyData 0.9 <!--DO NOT CHANGE RELEASE NO. -->. Although it does work, it is recommended to use `PERSISTENCE` instead.

   * <b>DISKSTORE:</b>  The disk directory where you want to persist the table data. For more information, [refer to this document](../reference/sql_reference/create-diskstore.md).

   * <b>EXPIRE:</b>  You can use the EXPIRE clause with tables to control the SnappyStore memory usage. It expires the rows after configured TTL.

   * <b>COLUMN_BATCH_SIZE:</b>  The default size of blocks to use for storage in the SnappyData column store. When inserting data into the column storage this is the unit (in bytes) that is used to split the data into chunks for efficient storage and retrieval. The default value is 25165824 (24M)

   * <b>COLUMN_MAX_DELTA_ROWS:</b>  The maximum number of rows that can be in the delta buffer of a column table for each bucket, before it is flushed into the column store. Although the size of column batches is limited by `COLUMN_BATCH_SIZE` (and thus limits the size of row buffer for each bucket as well), this property allows a lower limit on the number of rows for better scan performance. The default value is 10000. </br>
	 
    !!! Note: 
        The following corresponding SQLConf properties for `COLUMN_BATCH_SIZE` and `COLUMN_MAX_DELTA_ROWS` are set if the table creation is done in that session (and the properties have not been explicitly specified in the DDL): 
    	
		* `snappydata.column.batchSize` - explicit batch size for this session for bulk insert operations. If a table is created in the session without any explicit `COLUMN_BATCH_SIZE` specification, then this is inherited for that table property. 

    	* `snappydata.column.maxDeltaRows` - the maximum limit on rows in the delta buffer for each bucket of column table in this session. If a table is created in the session without any explicit `COLUMN_MAX_DELTA_ROWS` specification, then this is inherited for that table property. 

   Refer to the [SQL Reference Guide](../sql_reference.md) for information on the extensions.


### Restrictions on Column Tables

* Column tables cannot specify any primary key, unique key constraints

* Index on column table is not supported

* Option EXPIRE is not applicable for column tables

* Option EVICTION_BY with value LRUCOUNT is not applicable for column tables

* READ_COMMITTED and REPEATABLE_READ isolation levels are not supported for column tables.


### DML Operations on Tables

```scala
INSERT OVERWRITE TABLE tablename1 select_statement1 FROM from_statement;
INSERT INTO TABLE tablename1 select_statement1 FROM from_statement;
INSERT INTO TABLE tablename1 VALUES (value1, value2 ..) ;
UPDATE tablename SET column = value [, column = value ...] [WHERE expression]
PUT INTO tableName (column, ...) VALUES (value, ...)
DELETE FROM tablename1 [WHERE expression]
TRUNCATE TABLE tablename1;
```

### API Extensions Provided in SnappyContext
Several APIs have been added in [SnappySession](http://snappydatainc.github.io/snappydata/apidocs/#org.apache.spark.sql.SnappySession) to manipulate data stored in row and column format. Apart from SQL, these APIs can be used to manipulate tables.

```scala
//  Applicable for both row and column tables
def insert(tableName: String, rows: Row*): Int .

// Only for row tables
def put(tableName: String, rows: Row*): Int
def update(tableName: String, filterExpr: String, newColumnValues: Row, 
           updateColumns: String*): Int
def delete(tableName: String, filterExpr: String): Int
```

**Usage SnappySession.insert()**: Insert one or more [[org.apache.spark.sql.Row]] into an existing table

```scala
val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3),
               Seq(5, 6, 7), Seq(1,100,200))
data.map { r =>
  snappy.insert("tableName", Row.fromSeq(r))
}
```

**Usage SnappySession.put()**: Upsert one or more [[org.apache.spark.sql.Row]] into an existing table

```scala
val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3),
               Seq(5, 6, 7), Seq(1,100,200))
data.map { r =>
  snappy.put(tableName, Row.fromSeq(r))
}
```scala

Usage SnappySession.update(): Update all rows in table that match passed filter expression

```scala
snappy.update(tableName, "ITEMREF = 3" , Row(99) , "ITEMREF" )
```

**Usage SnappySession.delete()**: Delete all rows in table that match passed filter expression

```scala
snappy.delete(tableName, "ITEMREF = 3")
```

<!--
### String/CHAR/VARCHAR Data Types
SnappyData supports CHAR and VARCHAR datatypes in addition to Spark's String datatype. For performance reasons, it is recommended that you use either CHAR or VARCHAR type, if your column data fits in maximum CHAR size (254) or VARCHAR size (32768), respectively. For larger column data size, String type should be used as product stores its data in CLOB format internally.

**Create a table with columns of CHAR and VARCHAR datatype using SQL**:
```Scala
CREATE TABLE tableName (Col1 char(25), Col2 varchar(100)) using row;
```

**Create a table with columns of CHAR and VARCHAR datatype using API**:
```Scala
    import org.apache.spark.sql.collection.Utils
    import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

    val snappy: SnappySession = new SnappySession(spark.sparkContext)
    
    // define schema for table
    val varcharSize = 100
    val charSize = 25
    val schema = StructType(Array(
      StructField("col_varchar", StringType, false, Utils.varcharMetadata(varcharSize)),
      StructField("col_char", StringType, false, Utils.charMetadata(charSize))
    ))
    
    // create the table
    snappy.createTable(tableName, "row", schema, Map.empty[String, String])
```


!!! Note: 
	STRING columns are handled differently when queried over a JDBC connection.

To ensure optimal performance for SELECT queries executed over JDBC connection (more specifically, those that get routed to lead node), the data of STRING columns is returned in VARCHAR format, by default. This also helps the data visualization tools to render the data effectively.
<br/>However, if the STRING column size is larger than VARCHAR limit (32768), you can enforce the returned data format to be in CLOB in following ways:


Using the system property `spark-string-as-clob` when starting the lead node(s). This applies to all the STRING columns in all the tables in cluster.

```
bin/snappy leader start -locators:localhost:10334 -J-Dspark-string-as-clob=true
```

Defining the column(s) itself as CLOB, either using SQL or API. In the example below, column 'Col2' is defined as CLOB.

```
CREATE TABLE tableName (Col1 INT, Col2 CLOB, Col3 STRING, Col4 STRING);
```
```scala
    import org.apache.spark.sql.collection.Utils
    import org.apache.spark.sql.types.{StringType, StructField, StructType}

    val snappy: SnappySession = new SnappySession(spark.sparkContext)

    // Define schema for table
    val schema = StructType(Array(
      // The parameter Utils.stringMetadata() ensures that this column is rendered as CLOB
      StructField("Col2", StringType, false, Utils.stringMetadata())
    ))

    snappy.createTable(tableName, "column", schema, Map.empty[String, String])
```

Using the query-hint `columnsAsClob in the SELECT query.

```
SELECT * FROM tableName --+ columnsAsClob(*)
```
The usage of `*` above causes all the STRING columns in the table to be rendered as CLOB. You can also provide comma-separated specific column name(s) instead of `*` above so that data of only those column(s) is returned as CLOB.
```
SELECT * FROM tableName --+ columnsAsClob(Col3,Col4)
```
-->
### Row Buffers for Column Tables

Generally, the column table is used for analytical purpose. To this end, most of the operations (read or write) on it are bulk operations. Taking advantage of this fact the rows are compressed column wise and stored.

In SnappyData, the column table consists of two components, delta row buffer and column store. SnappyData tries to support individual insert of a single row, as it is stored in a delta row buffer which is write optimized and highly available.

Once the size of buffer reaches the COLUMN_BATCH_SIZE set by the user, the delta row buffer is compressed column wise and stored in the column store.
Any query on column table also takes into account the row cached buffer. By doing this, it ensures that the query does not miss any data.

### SQL Reference to the Syntax

Refer to the [SQL Reference Guide](../sql_reference.md) for information on the syntax.

