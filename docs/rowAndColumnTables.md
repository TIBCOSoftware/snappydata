## Row and column tables
SnappyStore provides two types of table. One is row format and other one is column format.
//Put some detail about structure & design etc.
### DDL and DML Syntax for tables

    CREATE TABLE [IF NOT EXISTS] table_name
       (
      COLUMN_DEFININTION
       )
    USING 'row | column'
    OPTIONS (
    COLOCATE_WITH 'table_name',  // Default none
    PARTITION_BY 'PRIMARY KEY | column name', // If not specified it will be a replicated table.
    BUCKETS  'NumPartitions', // Default 113
    REDUNDANCY        '1' ,
    RECOVER_DELAY     '-1',
    MAX_PART_SIZE      '50',
    EVICTION_BY  ‘MEMSIZE 200 | COUNT 200 | HEAPPERCENT,
    PERSISTENT   ‘DISKSTORE_NAME ASYNCHRONOUS |  SYNCHRONOUS’, // Empty string will map to default disk store.
    OFFHEAP ‘true | false’ ,
    EXPIRE ‘TIMETOLIVE in seconds',
    )
    [AS select_statement];

    DROP TABLE [IF EXISTS] table_name

For row format tables column definition can take underlying GemFire XD syntax to create a table.e.g.note the PRIMARY KEY clause below.

    snc.sql("CREATE TABLE tableName (Col1 INT NOT NULL PRIMARY KEY, Col2 INT, Col3 INT) USING row options()" )

But for column table its restricted to Spark syntax for column definition e.g.

    snc.sql("CREATE TABLE tableName (Col1 INT ,Col2 INT, Col3 INT) USING column options()" )

##### Spark API for managing tables

Get a reference to SnappyContext

    val snc: SnappyContext = SnappyContext.getOrCreate(sparkContext)

Create a SnappyStore table using Spark APIs

    val props = Map.empty[String, String] // This map should contain all the DDL extensions, described below
    case class Data(col1: Int, col2: Int, col3: Int)
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    snc.createTable("column_table", "column", dataDF.schema, props)
    //or create a row format table
    snc.createTable("row_table", "row", dataDF.schema, props)

Drop a SnappyStore table using Spark APIs

    snc.dropTable(tableName, ifExists = true)

#### DDL extensions to SnappyStore tables

   1. COLOCATE_WITH  : The COLOCATE_WITH clause specifies a partitioned table with which the new partitioned table must be colocated. The referenced table must already exist
   2. PARTITION_BY  : Use the PARTITION_BY {COLUMN} clause to provide a set of column names that will determine the partitioning. As a shortcut you can use PARTITION BY PRIMARY KEY to refer to the primary key columns defined for the table . If not specified it will be a replicated table.
   3. BUCKETS  : The optional BUCKETS attribute specifies the fixed number of "buckets," the smallest unit of data containment for the table that can be moved around. Data in a single bucket resides and moves together. If not specified, the number of buckets defaults to 113.
   4. REDUNDANCY : Use the REDUNDANCY clause to specify the number of redundant copies that should be maintained for each partition, to ensure that the partitioned table is highly available even if members fail.
   5. RECOVER_DELAY : Use the RECOVERY_DELAY clause to specify the default time in milliseconds that existing members will wait before satisfying redundancy after a member crashes. The default is -1, which indicates that redundancy is not recovered after a member fails.
   6. MAX_PART_SIZE : The MAXPARTSIZE attribute specifies the maximum memory for any partition on a member in megabytes. Use it to load-balance partitions among available members. If you omit MAXPARTSIZE, then GemFire XD calculates a default value for the table based on available heap memory. You can view the MAXPARTSIZE setting by querying the EVICTIONATTRS column in SYSTABLES.
   7. EVICTION_BY : Use the EVICTION_BY clause to evict rows automatically from the in-memory table based on different criteria. You can use this clause to create an overflow table where evicted rows are written to a local SnappyStore disk store
   8. PERSISTENT :  When you specify the PERSISTENT keyword, GemFire XD persists the in-memory table data to a local GemFire XD disk store configuration. SnappyStore automatically restores the persisted table data to memory when you restart the member.
   9. OFFHEAP : SnappyStore enables you to store the data for selected tables outside of the JVM heap. Storing a table in off-heap memory can improve performance for the table by reducing the CPU resources required to manage the table's data in the heap (garbage collection)
   10.  EXPIRE: You can use the EXPIRE clause with tables to control SnappyStore memory usage. It will expire the rows after configured TTL.

### Persistent tables
> Note: Restrictions on column tables in the preview release
#### DML operations on tables

    INSERT OVERWRITE TABLE tablename1 select_statement1 FROM from_statement;
    INSERT INTO TABLE tablename1 select_statement1 FROM from_statement;
    INSERT INTO TABLE tablename1 VALUES (value1, value2 ..) ;
    UPDATE tablename SET column = value [, column = value ...] [WHERE expression]
    DELETE FROM tablename1 [WHERE expression]
    TRUNCATE TABLE tablename1;

##### API extensions provided in SnappyContext
We have added several APIs in SnappyContext to manipulate data stored in row and column format. Apart from SQL these APIs can be used to manipulate tables.

    //  Applicable for both row & column tables
    def insert(tableName: String, rows: Row*): Int .

    // Only for row tables
    def put(tableName: String, rows: Row*): Int
    def update(tableName: String, filterExpr: String, newColumnValues: Row, updateColumns: String*): Int
    def delete(tableName: String, filterExpr: String): Int

Usage SnappyConytext.insert(): Insert one or more [[org.apache.spark.sql.Row]] into an existing table

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7), Seq(1,100,200))
    data.map { r =>
      snappyContext.insert("tableName", Row.fromSeq(r))
    }

Usage SnappyConytext.put(): Upsert one or more [[org.apache.spark.sql.Row]] into an existing table

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7), Seq(1,100,200))
    data.map { r =>
      snc.put(tableName, Row.fromSeq(r))
    }

Usage SnappyConytext.update(): Update all rows in table that match passed filter expression

    snc.update(tableName, "ITEMREF = 3" , Row(99) , "ITEMREF" )

Usage SnappyConytext.delete(): Delete all rows in table that match passed filter expression

    snc.delete(tableName, "ITEMREF = 3")


Explain the delta row buffer and how queries are executed

##### A note on how the catalog is managed in SnappyData
##### SQL Reference to the Syntax
For detailed syntax for GemFire XD check
http://gemfirexd.docs.pivotal.io/docs-gemfirexd/reference/sql-language-reference.html



