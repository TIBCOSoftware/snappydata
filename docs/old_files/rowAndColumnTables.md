### Row and column tables
Column tables organize and manage data in memory in compressed columnar form such that modern day CPUs can traverse and run computations like a sum or an average really fast (as the values are available in contiguous memory). Column table follows the Spark DataSource access model.

Row tables, unlike column tables, are laid out one row at a time in contiguous memory. Rows are typically accessed using keys and its location determined by a hash function and hence very fast for point lookups or updates.

Create table DDL for Row and Column tables allows tables to be partitioned on primary keys, custom partitioned, replicated, carry indexes in memory, persist to disk, overflow to disk, be replicated for HA, etc.

#### DDL and DML Syntax for tables

    CREATE TABLE [IF NOT EXISTS] table_name
       (
      COLUMN_DEFININTION
       )
    USING 'row | column'
    OPTIONS (
    COLOCATE_WITH 'table_name',  // Default none
    PARTITION_BY 'PRIMARY KEY | column name', // If not specified it will be a replicated table.
    BUCKETS  'NumPartitions', // Default 128
    REDUNDANCY        '1' ,
    RECOVER_DELAY     '-1',
    MAX_PART_SIZE      '50',
    EVICTION_BY ‘LRUMEMSIZE 200 | LRUCOUNT 200 | LRUHEAPPERCENT,
    PERSISTENT  ‘DISKSTORE_NAME ASYNCHRONOUS | SYNCHRONOUS’, //empty string will map to default diskstore
    OFFHEAP ‘true | false’ ,
    EXPIRE ‘TIMETOLIVE in seconds',
    )
    [AS select_statement];

    DROP TABLE [IF EXISTS] table_name

For row format tables column definition can take underlying GemFire XD syntax to create a table.e.g.note the PRIMARY KEY clause below.

    snc.sql("CREATE TABLE tableName (Col1 INT NOT NULL PRIMARY KEY, Col2 INT, Col3 INT)
             USING row options(BUCKETS '8')" )

But for column table it's restricted to Spark syntax for column definition e.g.

    snc.sql("CREATE TABLE tableName (Col1 INT ,Col2 INT, Col3 INT) USING column options(BUCKETS '8')" )
Clauses like PRIMARY KEY, NOT NULL etc. are not supported for column definition.

##### Spark API for managing tables

Get a reference to [SnappyContext](http://snappydatainc.github.io/snappydata/apidocs/#org.apache.spark.sql.SnappyContext)

    val snc: SnappyContext = SnappyContext.getOrCreate(sparkContext)

Create a SnappyStore table using Spark APIs

    val props = Map('BUCKETS','5') //This map should contain required DDL extensions, see next section
    case class Data(col1: Int, col2: Int, col3: Int)
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    snc.createTable("column_table", "column", dataDF.schema, props)
    //or create a row format table
    snc.createTable("row_table", "row", dataDF.schema, props)

Drop a SnappyStore table using Spark APIs

    snc.dropTable(tableName, ifExists = true)

##### DDL extensions to SnappyStore tables
The below mentioned DDL extensions are required to configure a table based on user requirements. One can specify one or more options to create the kind of table one wants. If no option is specified, default values are attached. See next section for various restrictions. 

   1. COLOCATE_WITH  : The COLOCATE_WITH clause specifies a partitioned table with which the new partitioned table must be colocated. The referenced table must already exist.
   2. PARTITION_BY  : Use the PARTITION_BY {COLUMN} clause to provide a set of column names that will determine the partitioning. As a shortcut you can use PARTITION BY PRIMARY KEY to refer to the primary key columns defined for the table . If not specified, it will be a replicated table.
   3. BUCKETS  : The optional BUCKETS attribute specifies the fixed number of "buckets," the smallest unit of data containment for the table that can be moved around. Data in a single bucket resides and moves together. If not specified, the number of buckets defaults to 128.
   4. REDUNDANCY : Use the REDUNDANCY clause to specify the number of redundant copies that should be maintained for each partition, to ensure that the partitioned table is highly available even if members fail.
   5. RECOVER_DELAY : Use the RECOVERY_DELAY clause to specify the default time in milliseconds that existing members will wait before satisfying redundancy after a member crashes. The default is -1, which indicates that redundancy is not recovered after a member fails.
   6. MAX_PART_SIZE : The MAXPARTSIZE attribute specifies the maximum memory for any partition on a member in megabytes. Use it to load-balance partitions among available members. If you omit MAXPARTSIZE, then GemFire XD calculates a default value for the table based on available heap memory. You can view the MAXPARTSIZE setting by querying the EVICTIONATTRS column in SYSTABLES.
   7. EVICTION_BY : Use the EVICTION_BY clause to evict rows automatically from the in-memory table based on different criteria. You can use this clause to create an overflow table where evicted rows are written to a local SnappyStore disk store
   8. PERSISTENT :  When you specify the PERSISTENT keyword, GemFire XD persists the in-memory table data to a local GemFire XD disk store configuration. SnappyStore automatically restores the persisted table data to memory when you restart the member.
   9. OFFHEAP : SnappyStore enables you to store the data for selected tables outside of the JVM heap. Storing a table in off-heap memory can improve performance for the table by reducing the CPU resources required to manage the table's data in the heap (garbage collection)
   10.  EXPIRE: You can use the EXPIRE clause with tables to control SnappyStore memory usage. It will expire the rows after configured TTL.

##### Restrictions on column tables
1. Column tables can not specify any primary key, unique key constraints.
2. Index on column table is not supported.
2. Option EXPIRE is not applicable for column tables.
3. Option EVICTION_BY with value LRUCOUNT is not applicable for column tables. 


#### DML operations on tables
   
    INSERT OVERWRITE TABLE tablename1 select_statement1 FROM from_statement;
    INSERT INTO TABLE tablename1 select_statement1 FROM from_statement;
    INSERT INTO TABLE tablename1 VALUES (value1, value2 ..) ;
    UPDATE tablename SET column = value [, column = value ...] [WHERE expression]
    PUT INTO tableName (column, ...) VALUES (value, ...)
    DELETE FROM tablename1 [WHERE expression]
    TRUNCATE TABLE tablename1;

##### API extensions provided in SnappyContext
We have added several APIs in [SnappyContext](http://snappydatainc.github.io/snappydata/apidocs/#org.apache.spark.sql.SnappyContext) to manipulate data stored in row and column format. Apart from SQL these APIs can be used to manipulate tables.

    //  Applicable for both row & column tables
    def insert(tableName: String, rows: Row*): Int .

    // Only for row tables
    def put(tableName: String, rows: Row*): Int
    def update(tableName: String, filterExpr: String, newColumnValues: Row, 
               updateColumns: String*): Int
    def delete(tableName: String, filterExpr: String): Int

Usage SnappyConytext.insert(): Insert one or more [[org.apache.spark.sql.Row]] into an existing table

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3),
                   Seq(5, 6, 7), Seq(1,100,200))
    data.map { r =>
      snappyContext.insert("tableName", Row.fromSeq(r))
    }

Usage SnappyConytext.put(): Upsert one or more [[org.apache.spark.sql.Row]] into an existing table

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3),
                   Seq(5, 6, 7), Seq(1,100,200))
    data.map { r =>
      snc.put(tableName, Row.fromSeq(r))
    }

Usage SnappyConytext.update(): Update all rows in table that match passed filter expression

    snc.update(tableName, "ITEMREF = 3" , Row(99) , "ITEMREF" )

Usage SnappyConytext.delete(): Delete all rows in table that match passed filter expression

    snc.delete(tableName, "ITEMREF = 3")


##### Row Buffers for column tables

Generally, the Column table is used for analytical purpose. To this end, most of the
operations (read or write) on it are bulk operations. Taking advantage of this fact
the rows are compressed column wise and stored.

In SnappyData, the column table consists of two components, delta row buffer and
column store. We try to support individual insert of single row, we store them in
a delta row buffer which is write optimized and highly available.
Once the size of buffer reaches the COLUMN_BATCH_SIZE set by user, the delta row
buffer is compressed column wise and stored in the column store.

Any query on column table, also takes into account the row cached buffer. By doing
this, we ensure that the query doesn't miss any data.

##### Catalog in SnappyStore
We use a persistent Hive catalog for all our metadata storage. All table, schema definition are stored here in a reliable manner. As we intend be able to quickly recover from driver failover, we chose GemFireXd itself to store meta information. This gives us ability to query underlying GemFireXD to reconstruct the metastore incase of a driver failover. 

There are pending work towards unifying DRDA & Spark layer catalog, which will part of future releases. 
##### SQL Reference to the Syntax
For detailed syntax for GemFire XD check
http://gemfirexd.docs.pivotal.io/docs-gemfirexd/reference/sql-language-reference.html



