# CREATE TABLE

## SYNTAX

**To Create Row/Column Table:**

```
CREATE TABLE [IF NOT EXISTS] table_name {
( { column-definition | table-constraint }
[ , { column-definition | table-constraint } ] * )
}
    USING row | column 
    OPTIONS (
    COLOCATE_WITH 'table-name',  // Default none
    PARTITION_BY 'column-name', // If not specified it will be a replicated table.
    BUCKETS  'num-partitions', // Default 113. Must be an integer.
    REDUNDANCY        'num-of-copies' , // Must be an integer
    EVICTION_BY ‘LRUMEMSIZE integer-constant | LRUCOUNT interger-constant | LRUHEAPPERCENT',
    PERSISTENT  ‘ASYNCHRONOUS | SYNCHRONOUS’,
    OVERFLOW 'true | false', // specifies the action to be executed upon eviction event
    EXPIRE ‘time_to_live_in_seconds',
    COLUMN_BATCH_SIZE 'column-batch-size-in-bytes', // Must be an integer. Only for column table
    COLUMN_MAX_DELTA_ROWS 'number-of-rows-in-each-bucket', // Must be an integer. Only for column table.
	)
	[AS select_statement];
```

!!! Note
	For more information on creating sample tables, refer to [Create Sample Table](create-sample-table.md).
    

//Jags>> the column_definitoon and table_constraint above should link to location where we discuss all the data types. Critically important.

## Description

Tables created using the standard SQL syntax without any of SnappyData specific extensions are created as row-oriented replicated tables. Thus, each data server node in the cluster hosts a consistent replica of the table. All tables are also registered in the Spark catalog and hence visible as DataFrames.

For example, `create table if not exists Table1 (a int)` is equivalent to `create table if not exists Table1 (a int) using row`.

//Jags>> The syntax should be 'create [temporary] table ...' ... we allow temporary tables. Need to verify correctness and confusion with spark temporary tables. 

<a id="ddl"></a>
## DDL Extensions
Below are the SnappyData specific extensions. You will find detailed usage examples in a later section. 

   * COLOCATE_WITH: The COLOCATE_WITH clause specifies a partitioned table to colocate with. The referenced table must already exist. 

   * PARTITION_BY: Use the PARTITION_BY {COLUMN} clause to provide a set of column names that determines the partitioning. As a shortcut you can use PARTITION BY PRIMARY KEY to refer to the primary key columns defined for the table. 

   * BUCKETS: The optional BUCKETS attribute specifies the fixed number of "buckets" to use for partitioned row or column tables. Each data server JVM manages one or more buckets. A bucket is a container of data and is the smallest unit of partitioning and migration in the system. For instance, in a cluster of 5 nodes and bucket count of 25 would result in 5 buckets on each node. But, if you configured the reverse - 25 nodes and a bucket count of 5, only 5 data servers will host all the data for this table. If not specified, the number of buckets defaults to 113.

   * REDUNDANCY: Use the REDUNDANCY clause to specify the number of redundant copies that should be maintained for each partition, to ensure that the partitioned table is highly available even if members fail. It is important to note that a redundancy of '1' implies two physical copies of data. 

   * EVICTION_BY: Use the EVICTION_BY clause to evict rows automatically from the in-memory table based on different criteria. You can use this clause to create an overflow table where evicted rows are written to a local SnappyStore disk store. It is important to note that all column tables (expected to host larger data sets) overflow to disk, by default. 

   * PERSISTENT: Persists the in-memory table data to a local SnappyData disk store. i.e. each node in the cluster will persist its managed partitions to configured local disk. By default, SnappyData creates a "default" disk store on each member node. SnappyStore automatically restores the persisted table data to memory when the member is restarted.

   * DISKSTORE: The disk directories where you want to persist the table data. You can use this option to control the location where data will be stored. For instance, you may decide to use a network file system or specify multiple disk mount points to uniformly scatter the data across disks. For more information, [refer to this document](create-diskstore.md).

   * OVERFLOW: Use the OVERFLOW clause to specify the action to be taken upon the eviction event. For persistent tables, setting this to 'true' will overflow the table evicted rows to disk based on the EVICTION_BY criteria . Setting this to 'false' will cause the evicted rows to be destroyed in case of eviction event.

   * EXPIRE: use the EXPIRE clause with tables to control the SnappyStore memory usage. It expires the rows after configured TTL.

//Jags>> Expire is not clear. Rishi or someone needs to be specify the correct semantics. Where is the action specification? do we just destroy? <mark>RISHI </mark>

   * COLUMN_BATCH_SIZE: The default size of blocks to use for storage in the SnappyData column store. When inserting data into the column storage this is the unit (in bytes) that is used to split the data into chunks for efficient storage and retrieval. The default value is 25165824 (24M)
   
   //Jags>> Shyja, please make a note that we need a separate section where we capture our memory management design. And, this ref should link to that section. Essentially, how does the Snappy storage system work? For each partition there is DeltaRowBuffer --> column store (split into multiple batches). Now, we default on a size not count ? ... Rishi, sumedh need to help. <mark>RISHI </mark>
   

   * COLUMN_MAX_DELTA_ROWS: The maximum number of rows that can be in the delta buffer of a column table for each bucket, before it is flushed into the column store. Although the size of column batches is limited by COLUMN_BATCH_SIZE (and thus limits size of row buffer for each bucket as well), this property allows a lower limit on the number of rows for better scan performance. The default value is 10000. 

	!!! Note
		The following corresponding SQLConf properties for `COLUMN_BATCH_SIZE` and `COLUMN_MAX_DELTA_ROWS` are set if the table creation is done in that session (and the properties have not been explicitly specified in the DDL): 

		* `snappydata.column.batchSize` - explicit batch size for this session for bulk insert operations. If a table is created in the session without any explicit `COLUMN_BATCH_SIZE` specification, then this is inherited for that table property. 

		* `snappydata.column.maxDeltaRows` - maximum limit on rows in the delta buffer for each bucket of column table in this session. If a table is created in the session without any explicit COLUMN_MAX_DELTA_ROWS specification, then this is inherited for that table property.

Refer to the [CREATE SAMPLE TABLE](create-sample-table.md) for information on the extensions applicable to sample tables.

## Example: Column Table Partitioned on a Single Column
```
 snappy>CREATE TABLE CUSTOMER ( 
        C_CUSTKEY     INTEGER NOT NULL,
        C_NAME        VARCHAR(25) NOT NULL,
        C_ADDRESS     VARCHAR(40) NOT NULL,
        C_NATIONKEY   INTEGER NOT NULL,
        C_PHONE       VARCHAR(15) NOT NULL,
        C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
        C_MKTSEGMENT  VARCHAR(10) NOT NULL,
        C_COMMENT     VARCHAR(117) NOT NULL))
        USING COLUMN OPTIONS (PARTITION_BY 'C_CUSTKEY');
```

## Example: Replicated, Persistent Row Table
```
	snappy>CREATE TABLE SUPPLIER ( 
          S_SUPPKEY INTEGER NOT NULL PRIMARY KEY, 
          S_NAME STRING NOT NULL, 
          S_ADDRESS STRING NOT NULL, 
          S_NATIONKEY INTEGER NOT NULL, 
          S_PHONE STRING NOT NULL, 
          S_ACCTBAL DECIMAL(15, 2) NOT NULL,
          S_COMMENT STRING NOT NULL)
          USING ROW OPTIONS (PERSISTENT);
```

//Jags>> We need a lot more examples. This section will be the most referenced one followed by SELECT ...


## Example Create Table using Query

```
CREATE TABLE CUSTOMER_STAGING USING COLUMN OPTIONS (PARTITION_BY 'C_CUSTKEY') AS SELECT * FROM CUSTOMER ;
```
With this alternate form of the CREATE TABLE statement, you specify the column names and/or the column data types with a query. The columns in the query result are used as a model for creating the columns in the new table.

If no column names are specified for the new table, then all the columns in the result of the query expression are used to create same-named columns in the new table, of the corresponding data type(s). If one or more column names are specified for the new table, the same number of columns must be present in the result of the query expression; the data types of those columns are used for the corresponding columns of the new table.

Note that only the column names and datatypes from the queried table are used when creating the new table. Additional settings in the queried table, such as partitioning, replication, and persistence, are not duplicated. You can optionally specify partitioning, replication, and persistence configuration settings for the new table, and those settings need not match the settings of the queried table.

//Jags>> Definately need examples with external tables .... how to load from S3, HDFS, CSV, etc .... (maybe in 'data Loading' section)

//Jags>> link to section in docs where we can create tables using Spark API, loading from existing DataFrames, etc.
