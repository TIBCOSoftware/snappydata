<a id="markdown_link_row_and_column_tables"></a>

## Tables in SnappyData
### Row and Column Tables
Column tables organize and manage data in memory in compressed columnar form such that, modern day CPUs can traverse and run computations like a sum or an average really fast (as the values are available in contiguous memory). Column table follows the Spark DataSource access model.

Row tables, unlike column tables, are laid out one row at a time in contiguous memory. Rows are typically accessed using keys and its location is determined by a hash function and hence is fast for point lookups or updates.


Create table DDL for Row and Column tables allows tables to be partitioned on primary keys, custom partitioned, replicated, carry indexes in memory, persist to disk, overflow to disk, be replicated for HA, etc.

#### DDL and DML Syntax for Tables
```sql
CREATE TABLE [IF NOT EXISTS] table_name
   (
  COLUMN_DEFININTION
   )
USING row | column
OPTIONS (
COLOCATE_WITH 'table_name',  // Default none
PARTITION_BY 'PRIMARY KEY | column name', // If not specified it will be a replicated table.
BUCKETS  'NumPartitions', // Default 113
REDUNDANCY        '1' ,
EVICTION_BY ‘LRUMEMSIZE 200 | LRUCOUNT 200 | LRUHEAPPERCENT,
PERSISTENT  ‘DISKSTORE_NAME ASYNCHRONOUS | SYNCHRONOUS’, //empty string maps to default diskstore
EXPIRE ‘TIMETOLIVE in seconds',
)
[AS select_statement];

DROP TABLE [IF EXISTS] table_name
```
Refer to the [How-Tos](howto) section for more information on partitioning and collocating data.

For row format tables column definition can take underlying GemFire XD syntax to create a table. For example, note the PRIMARY KEY clause below.

```scala
snappy.sql("CREATE TABLE tableName (Col1 INT NOT NULL PRIMARY KEY, Col2 INT, Col3 INT)
         USING row options(BUCKETS '5')" )
```
For column table it is restricted to Spark syntax for column definition
```scala
snappy.sql("CREATE TABLE tableName (Col1 INT ,Col2 INT, Col3 INT) USING column options(BUCKETS '5')" )
```

You can also define complex types (Map, Array and StructType) as columns for column tables.
```scala
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

#### Spark API for Managing Tables

**Get a reference to [SnappySession](http://snappydatainc.github.io/snappydata/apidocs/#org.apache.spark.sql.SnappySession):**

    val snappy: SnappySession = new SnappySession(spark.sparkContext)

Create a SnappyStore table using Spark APIs

    val props = Map('BUCKETS','5') //This map should contain required DDL extensions, see next section
    case class Data(col1: Int, col2: Int, col3: Int)
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sparkContext.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snappy.createDataFrame(rdd)
    snappy.createTable("column_table", "column", dataDF.schema, props)
    //or create a row format table
    snappy.createTable("row_table", "row", dataDF.schema, props)

**Drop a SnappyStore table using Spark APIs**:

    snappy.dropTable(tableName, ifExists = true)
    
<a id="ddl"></a>
#### DDL extensions to SnappyStore Tables
The below mentioned DDL extensions are required to configure a table based on user requirements. One can specify one or more options to create the kind of table one wants. If no option is specified, default values are attached. See next section for various restrictions. 

   1. COLOCATE_WITH: The COLOCATE_WITH clause specifies a partitioned table with which the new partitioned table must be colocated. The referenced table must already exist.

   2. PARTITION_BY: Use the PARTITION_BY {COLUMN} clause to provide a set of column names that determines the partitioning. As a shortcut you can use PARTITION BY PRIMARY KEY to refer to the primary key columns defined for the table. If not specified, it is a replicated table.

   3. BUCKETS: The optional BUCKETS attribute specifies the fixed number of "buckets," the smallest unit of data containment for the table that can be moved around. Data in a single bucket resides and moves together. If not specified, the number of buckets defaults to 113.

   4. REDUNDANCY: Use the REDUNDANCY clause to specify the number of redundant copies that should be maintained for each partition, to ensure that the partitioned table is highly available even if members fail.

   5. EVICTION_BY: Use the EVICTION_BY clause to evict rows automatically from the in-memory table based on different criteria. You can use this clause to create an overflow table where evicted rows are written to a local SnappyStore disk store

   6. PERSISTENT:  When you specify the PERSISTENT keyword, GemFire XD persists the in-memory table data to a local GemFire XD disk store configuration. SnappyStore automatically restores the persisted table data to memory when you restart the member.

   7. EXPIRE: You can use the EXPIRE clause with tables to control the SnappyStore memory usage. It expires the rows after configured TTL.
   
   Refer to the [SQL Reference Guide](#sql_reference/sql_reference.md) for information on the extensions.

	
#### Restrictions on Column Tables
* Column tables cannot specify any primary key, unique key constraints

* Index on column table is not supported

* Option EXPIRE is not applicable for column tables

* Option EVICTION_BY with value LRUCOUNT is not applicable for column tables


#### DML Operations on Tables
```   
    INSERT OVERWRITE TABLE tablename1 select_statement1 FROM from_statement;
    INSERT INTO TABLE tablename1 select_statement1 FROM from_statement;
    INSERT INTO TABLE tablename1 VALUES (value1, value2 ..) ;
    UPDATE tablename SET column = value [, column = value ...] [WHERE expression]
    PUT INTO tableName (column, ...) VALUES (value, ...)
    DELETE FROM tablename1 [WHERE expression]
    TRUNCATE TABLE tablename1;
```
#### API Extensions Provided in SnappyContext
Several APIs have been added in [SnappySession](http://snappydatainc.github.io/snappydata/apidocs/#org.apache.spark.sql.SnappySession) to manipulate data stored in row and column format. Apart from SQL these APIs can be used to manipulate tables.
```
    //  Applicable for both row and column tables
    def insert(tableName: String, rows: Row*): Int .

    // Only for row tables
    def put(tableName: String, rows: Row*): Int
    def update(tableName: String, filterExpr: String, newColumnValues: Row, 
               updateColumns: String*): Int
    def delete(tableName: String, filterExpr: String): Int
```
**Usage SnappySession.insert()**: Insert one or more [[org.apache.spark.sql.Row]] into an existing table
```
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3),
                   Seq(5, 6, 7), Seq(1,100,200))
    data.map { r =>
      snappy.insert("tableName", Row.fromSeq(r))
    }
```
**Usage SnappySession.put()**: Upsert one or more [[org.apache.spark.sql.Row]] into an existing table
```
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3),
                   Seq(5, 6, 7), Seq(1,100,200))
    data.map { r =>
      snappy.put(tableName, Row.fromSeq(r))
    }
```
**Usage SnappySession.update()**: Update all rows in table that match passed filter expression
```
    snappy.update(tableName, "ITEMREF = 3" , Row(99) , "ITEMREF" )
```
**Usage SnappySession.delete()**: Delete all rows in table that match passed filter expression
```
    snappy.delete(tableName, "ITEMREF = 3")
```

#### String/CHAR/VARCHAR Data Types
SnappyData supports CHAR and VARCHAR datatypes in addition to Spark's String datatype. For performance reasons, it is recommended that you use either CHAR or VARCHAR type, if your column data fits in maximum CHAR size (254) or VARCHAR size (32768), respectively. For larger column data size, String type should be used as we store its data in CLOB format internally.

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

!!! Note
	STRING columns are handled differently when queried over a JDBC connection.

To ensure optimal performance for SELECT queries executed over JDBC connection (more specifically, those that get routed to lead node), the data of STRING columns is returned in VARCHAR format, by default. This also helps the data visualization tools to render the data effectively.
<br/>However, if the STRING column size is larger than VARCHAR limit (32768), you can enforce the returned data format to be in CLOB in following ways:


Using the system property `spark-string-as-clob` when starting the lead node(s). This applies to all the STRING columns in all the tables in cluster.

```
bin/snappy leader start -locators:localhost:10334 -J-Dspark-string-as-clob=true
```

Defining the column(s) itself as CLOB, either using SQL or API. In the example below, we define the column 'Col2' to be CLOB.

```
CREATE TABLE tableName (Col1 INT, Col2 CLOB, Col3 STRING, Col4 STRING);
```
```Scala
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

#### Row Buffers for Column Tables

Generally, the column table is used for analytical purpose. To this end, most of the operations (read or write) on it are bulk operations. Taking advantage of this fact the rows are compressed column wise and stored.

In SnappyData, the column table consists of two components, delta row buffer and column store. We try to support individual insert of single row, we store them in a delta row buffer which is write optimized and highly available.
Once the size of buffer reaches the COLUMN_BATCH_SIZE set by the user, the delta row buffer is compressed column wise and stored in the column store.
Any query on column table also takes into account the row cached buffer. By doing this, we ensure that the query does not miss any data.

#### Catalog in SnappyStore
We use a persistent Hive catalog for all our metadata storage. All table, schema definition are stored here in a reliable manner. As we intend be able to quickly recover from driver failover, we chose GemFireXd itself to store meta information. This gives us the ability to query underlying GemFireXD to reconstruct the meta store in case of a driver failover.

<!--<mark>There are pending work towards unifying DRDA & Spark layer catalog, which will part of future releases. </mark>-->

#### SQL Reference to the Syntax

Refer to the [SQL Reference Guide](#sql_reference/sql_reference.md) for information on the syntax.
