# CREATE TABLE

Following is the syntax used to create a Row/Column table:

```pre
CREATE TABLE [IF NOT EXISTS] table_name 
    ( column-definition	[ , column-definition  ] * )	
    USING [row | column] // If not specified, a row table is created.
    OPTIONS (
    COLOCATE_WITH 'table-name',  // Default none
    PARTITION_BY 'column-name', // If not specified, replicated table for row tables, and partitioned internally for column tables.
    BUCKETS  'num-partitions', // Default 128. Must be an integer.
    COMPRESSION 'NONE', //By default COMPRESSION is 'ON'. 
    REDUNDANCY       'num-of-copies' , // Must be an integer. By default, REDUNDANCY is set to 0 (zero). '1' is recommended value. Maximum limit is '3'
    EVICTION_BY 'LRUMEMSIZE integer-constant | LRUCOUNT interger-constant | LRUHEAPPERCENT',
    PERSISTENCE  'ASYNCHRONOUS | ASYNC | SYNCHRONOUS | SYNC | NONE’,
    DISKSTORE 'DISKSTORE_NAME', //empty string maps to default diskstore
    OVERFLOW 'true | false', // specifies the action to be executed upon eviction event, 'false' allowed only when EVCITON_BY is not set.
    EXPIRE 'time_to_live_in_seconds',
    COLUMN_BATCH_SIZE 'column-batch-size-in-bytes', // Must be an integer. Only for column table.
	KEY_COLUMNS  'column_name,..', // Only for column table if putInto support is required
    COLUMN_MAX_DELTA_ROWS 'number-of-rows-in-each-bucket', // Must be an integer > 0 and < 2GB. Only for column table.
	)
	[AS select_statement];
```

Refer to the following sections for:

*	[Creating Sample Table](create-sample-table.md)
*	[Creating External Table](create-external-table.md)
*	[Creating Temporary Table](create-temporary-table.md)
*	[Creating Stream Table](create-stream-table.md).


## Column Definition

The column definition defines the name of a column and its data type.

<a id="column-definition"></a>
`column-definition` (for Column Table)

```pre
column-definition: column-name column-data-type [NOT NULL]

column-name: 'unique column name'
```

<a id="row-definition"></a>
`column-definition` (for Row Table)

```pre
column-definition: column-definition-for-row-table | table-constraint

column-definition-for-row-table: column-name column-data-type [ column-constraint ] *
    [ [ WITH ] DEFAULT { constant-expression | NULL } 
      | [ GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY
          [ ( START WITH start-value-as-integer [, INCREMENT BY step-value-as-integer ] ) ] ] ]
    [ column-constraint ] *
```

*	Refer to the [identity](#id-columns) section for more information on GENERATED.</br>
*	Refer to the [constraint](#constraint) section for more information on table-constraint and column-constraint.

### column-data-type
The following data types are supported:

```pre
column-data-type: 
	BIGINT |
	BINARY |
	BLOB |
	BOOLEAN |
	BYTE |
	CLOB |
	DATE |
	DECIMAL |
	DOUBLE |
	FLOAT |
	INT |
	INTEGER |
	LONG |
	NUMERIC |
	REAL |
	SHORT |
	SMALLINT |
	STRING |
	TIMESTAMP |
	TINYINT |
	VARBINARY |
	VARCHAR |
```

Column tables can also use **ARRAY**, **MAP** and **STRUCT** types.

Decimal and numeric has default precision of 38 and scale of 18.

## Using

You can specify if you want to create a row table or a column table. If this is not specified, a row table is created by default. 

## Options

You can specify the following options when you create a table:

+	[COLOCATE_WITH](#colocate-with)
+	[PARTITION_BY](#partition-by)
+	[BUCKETS](#buckets)
+	[COMPRESSION](#compress)
+	[REDUNDANCY](#redundancy)
+	[EVICTION_BY](#eviction-by)
+	[PERSISTENCE](#persistence)
+	[DISKSTORE](#diskstore)
+	[OVERFLOW](#overflow)
+	[EXPIRE](#expire)
+	[COLUMN_BATCH_SIZE](#column-batch-size)
+	[COLUMN_MAX_DELTA_ROWS](#column-max-delta-rows)

!!!Note
	If options are not specified, then the default values are used to create the table.

<a id="ddl"></a>
<a id="colocate-with"></a>
`COLOCATE_WITH`</br>
The COLOCATE_WITH clause specifies a partitioned table with which the new partitioned table must be colocated. 

<a id="partition-by"></a>
`PARTITION_BY`</br>
Use the PARTITION_BY {COLUMN} clause to provide a set of column names that determine the partitioning. </br>
If not specified, for row table (mentioned further for the case of column table) it is a 'replicated row table'.</br> 
Column and row tables support hash partitioning on one or more columns. These are specified as comma-separated column names in the PARTITION_BY option of the CREATE TABLE DDL or createTable API. The hashing scheme follows the Spark Catalyst Hash Partitioning to minimize shuffles in joins. If no PARTITION_BY option is specified for a column table, then, the table is still partitioned internally.</br> The default number of storage partitions (BUCKETS) is 128 in cluster mode for column and row tables, and 11 in local mode for column and partitioned row tables. This can be changed using the BUCKETS option in CREATE TABLE DDL or createTable API.

<a id="buckets"></a>
`BUCKETS` </br>
The optional BUCKETS attribute specifies the fixed number of "buckets" to use for the partitioned row or column tables. Each data server JVM manages one or more buckets. A bucket is a container of data and is the smallest unit of partitioning and migration in the system. For instance, in a cluster of five nodes and a bucket count of 25 would result in 5 buckets on each node. But, if you configured the reverse - 25 nodes and a bucket count of 5, only 5 data servers hosts all the data for this table. If not specified, the number of buckets defaults to 128. See [best practices](../../best_practices/optimizing_query_latency.md#partition-scheme) for more information. 
For row tables, `BUCKETS` must be created with the `PARTITION_BY` clause, else an error is reported.

<a id="compress"></a>
`COMPRESSION`</br>
Column tables use compression of data by default. This reduces the total storage footprint for large tables. SnappyData column tables encode data for compression and hence require memory that is less than or equal to the on-disk size of the uncompressed data. By default, compression is on for column tables. To disable data compression, you can set the COMPRESSION option to `none` when you create a table. For example:
```
CREATE TABLE AIRLINE USING column OPTIONS(compression 'none')  AS (select * from STAGING_AIRLINE);
```
See [best practices](../../best_practices/memory_management.md#estimating-memory-size-for-column-and-row-tables) for more information.

<a id="redundancy"></a>
`REDUNDANCY`</br>
Use the REDUNDANCY clause to specify the number of redundant copies that should be maintained for each partition, to ensure that the partitioned table is highly available even if members fail. It is important to note that redundancy of '1' implies two physical copies of data. By default, REDUNDANCY is set to 0 (zero). A REDUNDANCY value of '1' is recommended. A large value for REDUNDANCY clause has an adverse impact on performance, network usage, and memory usage. A maximum limit of '3' can be set for REDUNDANCY. See [best practices](../../best_practices/optimizing_query_latency.md#redundancy) for more information.

<a id="eviction-by"></a>
`EVICTION_BY`</br>
Use the EVICTION_BY clause to evict rows automatically from the in-memory table based on different criteria. You can use this clause to create an overflow table where evicted rows are written to a local SnappyStore disk store. It is important to note that all tables (expected to host larger data sets) overflow to disk, by default. See [best practices](../../best_practices/optimizing_query_latency.md#overflow) for more information. The value for this parameter is set in MB.
For column tables, the default eviction setting is `LRUHEAPPERCENT` and the default action is to overflow to disk. You can also specify the `OVERFLOW` parameter along with the `EVICTION_BY` clause.

!!! Note
	- EVICTION_BY is not supported for replicated tables.

	- For column tables, you cannot use the LRUMEMSIZE or LRUCOUNT eviction settings. For row tables, no such defaults are set. Row tables allow all the eviction settings.
    

<a id="persistence"></a>
`PERSISTENCE`</br>
When you specify the PERSISTENCE keyword, SnappyData persists the in-memory table data to a local SnappyData disk store configuration. SnappyStore automatically restores the persisted table data to memory when you restart the member. 

!!! Note

   	* By default, both row and column tables are persistent.

   	* The option `PERSISTENT` has been deprecated as of SnappyData 0.9. Although it does work, it is recommended to use `PERSISTENCE` instead.

<a id="diskstore"></a>
`DISKSTORE`</br>
The disk directories where you want to persist the table data. By default, SnappyData creates a "default" disk store on each member node. You can use this option to control the location where data is stored. For instance, you may decide to use a network file system or specify multiple disk mount points to uniformly scatter the data across disks. For more information, refer to [CREATE DISKSTORE](create-diskstore.md).


<a id="overflow"></a>
`OVERFLOW`</br> 
Use the OVERFLOW clause to specify the action to be taken upon the eviction event. For persistent tables, setting this to 'true' overflows the table evicted rows to disk based on the EVICTION_BY criteria. Setting this to 'false' is not allowed except when EVICTION_BY is set. In such case, the eviction itself is disabled.</br>
When you configure an overflow table, only the evicted rows are written to disk. If you restart or shut down a member that hosts the overflow table, the table data that was in memory is not restored unless you explicitly configure persistence (or you configure one or more replicas with a partitioned table).

!!! Note 
	The tables are evicted to disk by default, which means table data overflows to a local SnappyStore disk store.

<a id="expire"></a>
`EXPIRE`</br>
Use the EXPIRE clause with tables to control the SnappyStore memory usage. It expires the rows after configured `time_to_live_in_seconds`.

<a id="column-batch-size"></a>
`COLUMN_BATCH_SIZE`</br>
The default size of blocks to use for storage in the SnappyData column store. When inserting data into the column storage this is the unit (in bytes) that is used to split the data into chunks for efficient storage and retrieval. The default value is 25165824 (24M).

<a id="column-max-delta-rows"></a>
`COLUMN_MAX_DELTA_ROWS`</br>
The maximum number of rows that can be in the delta buffer of a column table for each bucket, before it is flushed into the column store. Although the size of column batches is limited by COLUMN_BATCH_SIZE (and thus limits the size of row buffer for each bucket as well), this property allows a lower limit on the number of rows for better scan performance. The value should be > 0 and < 2GB. The default value is 10000.

!!! Note
	The following corresponding SQLConf properties for `COLUMN_BATCH_SIZE` and `COLUMN_MAX_DELTA_ROWS` are set if the table creation is done in that session (and the properties have not been explicitly specified in the DDL):

	* `snappydata.column.batchSize` - Explicit batch size for this session for bulk insert operations. If a table is created in the session without any explicit `COLUMN_BATCH_SIZE` specification, then this is inherited for that table property.
	* `snappydata.column.maxDeltaRows` - The maximum limit on rows in the delta buffer for each bucket of column table in this session. If a table is created in the session without any explicit COLUMN_MAX_DELTA_ROWS specification, then this is inherited for that table property.

Tables created using the standard SQL syntax without any of SnappyData specific extensions are created as row-oriented replicated tables. Thus, each data server node in the cluster hosts a consistent replica of the table. All tables are also registered in the Spark catalog and hence visible as DataFrames.

For example, `create table if not exists Table1 (a int)` is equivalent to `create table if not exists Table1 (a int) using row`.

## Examples

### Example: Column Table Partitioned on a Single Column
```pre
snappy>CREATE TABLE CUSTOMER ( 
    C_CUSTKEY     INTEGER NOT NULL,
    C_NAME        VARCHAR(25) NOT NULL,
    C_ADDRESS     VARCHAR(40) NOT NULL,
    C_NATIONKEY   INTEGER NOT NULL,
    C_PHONE       VARCHAR(15) NOT NULL,
    C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
    C_MKTSEGMENT  VARCHAR(10) NOT NULL,
    C_COMMENT     VARCHAR(117) NOT NULL)
    USING COLUMN OPTIONS (BUCKETS '10', PARTITION_BY 'C_CUSTKEY');
```

### Example: Column Table Partitioned with 10 Buckets and Persistence Enabled
```pre
snappy>CREATE TABLE CUSTOMER ( 
    C_CUSTKEY     INTEGER NOT NULL,
    C_NAME        VARCHAR(25) NOT NULL,
    C_ADDRESS     VARCHAR(40) NOT NULL,
    C_NATIONKEY   INTEGER NOT NULL,
    C_PHONE       VARCHAR(15) NOT NULL,
    C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
    C_MKTSEGMENT  VARCHAR(10) NOT NULL,
    C_COMMENT     VARCHAR(117) NOT NULL)
    USING COLUMN OPTIONS (BUCKETS '10', PARTITION_BY 'C_CUSTKEY', PERSISTENCE 'SYNCHRONOUS');
```

### Example: Replicated, Persistent Row Table
```pre
snappy>CREATE TABLE SUPPLIER ( 
      S_SUPPKEY INTEGER NOT NULL PRIMARY KEY, 
      S_NAME STRING NOT NULL, 
      S_ADDRESS STRING NOT NULL, 
      S_NATIONKEY INTEGER NOT NULL, 
      S_PHONE STRING NOT NULL, 
      S_ACCTBAL DECIMAL(15, 2) NOT NULL,
      S_COMMENT STRING NOT NULL)
      USING ROW OPTIONS (PARTITION_BY 'S_SUPPKEY', BUCKETS '10', PERSISTENCE 'ASYNCHRONOUS');
```

### Example: Row Table Partitioned with 10 Buckets and Overflow Enabled
```pre
snappy>CREATE TABLE SUPPLIER ( 
      S_SUPPKEY INTEGER NOT NULL PRIMARY KEY, 
      S_NAME STRING NOT NULL, 
      S_ADDRESS STRING NOT NULL, 
      S_NATIONKEY INTEGER NOT NULL, 
      S_PHONE STRING NOT NULL, 
      S_ACCTBAL DECIMAL(15, 2) NOT NULL,
      S_COMMENT STRING NOT NULL)
      USING ROW OPTIONS (BUCKETS '10',
      PARTITION_BY 'S_SUPPKEY',
      PERSISTENCE 'ASYNCHRONOUS',
      EVICTION_BY 'LRUCOUNT 3',
      OVERFLOW 'true');
```

### Example: Create Table using Select Query

```pre
CREATE TABLE CUSTOMER_STAGING USING COLUMN OPTIONS (PARTITION_BY 'C_CUSTKEY') AS SELECT * FROM CUSTOMER ;
```

With this alternate form of the CREATE TABLE statement, you specify the column names and/or the column data types with a query. The columns in the query result are used as a model for creating the columns in the new table.

If no column names are specified for the new table, then all the columns in the result of the query expression are used to create same-named columns in the new table, of the corresponding data type(s). If one or more column names are specified for the new table, the same number of columns must be present in the result of the query expression; the data types of those columns are used for the corresponding columns of the new table.

!!!Note
	Only the column names and data types from the queried table are used when creating the new table. Additional settings in the queried table, such as partitioning, replication, and persistence, are not duplicated. You can optionally specify partitioning, replication, and persistence configuration settings for the new table and those settings need not match the settings of the queried table.

When you are creating a new table in SnappyData from another table, for example an external table, by using `CREATE TABLE ... AS SELECT * FROM ...` query and you find that the query fails with the message: `Syntax error or analysis exception: Table <schemaname.tablename> already exists ...`, it's likely due to insufficient memory on one of the servers and that the server is going down. 
You may also see an entry for that table created when you run `show tables` command immediately after.
This happens because some tasks pertaining to the query may be still running on another server(s). As soon as those tasks are completed, the table gets cleaned up as expected because the query failed.

### Example: Create Table using Spark DataFrame API

For information on using the Apache Spark API, refer to [Using the Spark DataFrame API](../../sde/running_queries.md#using-the-spark-dataframe-api).

### Example: Create Column Table with PUT INTO

```pre
snappy> CREATE TABLE COL_TABLE (
      PRSN_EVNT_ID BIGINT NOT NULL,
      VER bigint NOT NULL,
      CLIENT_ID BIGINT NOT NULL,
      SRC_TYP_ID BIGINT NOT NULL)
      USING COLUMN OPTIONS(PARTITION_BY 'PRSN_EVNT_ID,CLIENT_ID', BUCKETS '64', KEY_COLUMNS 'PRSN_EVNT_ID, CLIENT_ID');
```

### Example: Create Table with Eviction Settings

Use eviction settings to keep your table within a specified limit, either by removing evicted data completely or by creating an overflow table that persists the evicted data to a disk store.

1. Decide whether to evict based on:
	- Entry count (useful if table row sizes are relatively uniform).

	- Total bytes used.

	- Percentage of JVM heap used. This uses the SnappyData resource manager. When the manager determines that eviction is required, the manager orders the eviction controller to start evicting from all tables where the eviction criterion is set to LRUHEAPPERCENT.

2. Decide what action to take when the limit is reached:
	- Locally destroy the row (partitioned tables only).
	
	- Overflow the row data to disk.

3. If you want to overflow data to disk (or persist the entire table to disk), configure a named disk store to use for the overflow data. If you do not specify a disk store when creating an overflow table, SnappyData stores the overflow data in the default disk store.

4. Create the table with the required eviction configuration.

	For example, to evict using LRU entry count and overflow evicted rows to a disk store (OverflowDiskStore):
	
        CREATE TABLE Orders(OrderId INT NOT NULL,ItemId INT) USING row OPTIONS (EVICTION_BY 'LRUCOUNT 2', OVERFLOW 'true', DISKSTORE 'OverflowDiskStore', PERSISTENCE 'async');

	
    To create a table that simply removes evicted data from memory without persisting the evicted data, use the `DESTROY` eviction action. For example:
    Default in SnappyData for `synchronous` is `persistence`, `overflow` is `true` and `eviction_by` is `LRUHEAPPERCENT`.
    	
        CREATE TABLE Orders(OrderId INT NOT NULL,ItemId INT) USING row OPTIONS (PARTITION_BY 'OrderId', EVICTION_BY 'LRUMEMSIZE 1000');

### Example: Create Column Table with COMMENT Clause

You can add comments about a column using the COMMENT clause. The COMMENT clause must be used in the column definition as shown in the following example:
```
snappy> create table foobar (a string comment 'column 1', b string not null comment 'column 2') using column;
snappy> describe foobar;
col_name |data_type |comment
------------------------------
a        |string    |column 1
b        |string    |column 2
```

<a id="constraint"></a>
### Constraint (only for Row Tables)
A CONSTRAINT clause is an optional part of a CREATE TABLE statement that defines a rule to which table data must conform.

There are two types of constraints:</br>

**Column-level constraints**: Refer to a single column in the table and do not specify a column name (except check constraints). They refer to the column that they follow.</br>
**Table-level constraints**: Refer to one or more columns in the table. Table-level constraints specify the names of the columns to which they apply. Table-level CHECK constraints can refer to 0 or more columns in the table.

Column and table constraints include:

* NOT NULL— Specifies that a column cannot hold NULL values (constraints of this type are not nameable).

* PRIMARY KEY— Specifies a column (or multiple columns if specified in a table constraint) that uniquely identifies a row in the table. The identified columns must be defined as NOT NULL.

* UNIQUE— Specifies that values in the column must be unique. NULL values are not allowed.

* FOREIGN KEY— Specifies that the values in the columns must correspond to values in the referenced primary key or unique columns or that they are NULL. </br>If the foreign key consists of multiple columns and any column is NULL, then the whole key is considered NULL. SnappyData permits the insert no matter what is in the non-null columns.

* CHECK— Specifies rules for values in a column, or specifies a wide range of rules for values when included as a table constraint. The CHECK constraint has the same format and restrictions for column and table constraints.
Column constraints and table constraints have the same function; the difference is where you specify them. Table constraints allow you to specify more than one column in a PRIMARY KEY, UNIQUE, CHECK, or FOREIGN KEY constraint definition. 

Column-level constraints (except for check constraints) refer to only one column.
If you do not specify a name for a column or table constraint, then SnappyData generates a unique name.

**Example**: The following example demonstrates how to create a table with `FOREIGN KEY`: </br>

```pre
snappy> create table trading.customers (cid int not null, cust_name varchar(100), since date, addr varchar(100), tid int, primary key (cid));

snappy> create table trading.networth (cid int not null, cash decimal (30, 20), securities decimal (30, 20), loanlimit int, availloan decimal (30, 20),  tid int, constraint netw_pk primary key (cid), constraint cust_newt_fk foreign key (cid) references trading.customers (cid));

snappy> show importedkeys in trading;
PKTABLE_NAME        |PKCOLUMN_NAME       |PK_NAME             |FKTABLE_SCHEM       |FKTABLE_NAME        |FKCOLUMN_NAME       |FK_NAME             |KEY_SEQ
---------------------------------------------------------------------------------------------------------------------------------------------------------- 
CUSTOMERS           |CID                 |SQL180403162038220  |TRADING             |NETWORTH            |CID                 |CUST_NEWT_FK        |1  
```

<a id="id-columns"></a>
### Identity Columns (only for Row Tables)
<a id="generate"></a>
SnappyData supports both GENERATED ALWAYS and GENERATED BY DEFAULT identity columns only for BIGINT and INTEGER data types. The START WITH and INCREMENT BY clauses are supported only for GENERATED BY DEFAULT identity columns.

For a GENERATED ALWAYS identity column, SnappyData increments the default value on every insertion, and stores the incremented value in the column. You cannot insert a value directly into a GENERATED ALWAYS identity column, and you cannot update a value in a GENERATED ALWAYS identity column. Instead, you must either specify the DEFAULT keyword when inserting data into the table or you must leave the identity column out of the insertion column list.

Consider a table with the following column definition:

```pre
create table greetings (i int generated always as identity, ch char(50)) using row;
```

You can insert rows into the table using either the DEFAULT keyword or by omitting the identity column from the INSERT statement:

```pre
insert into greetings values (DEFAULT, 'hello');
```

```pre
insert into greetings(ch) values ('hi');
```

The values that SnappyData automatically generates for a GENERATED ALWAYS identity column are unique.

For a GENERATED BY DEFAULT identity column, SnappyData increments and uses a default value for an INSERT only when no explicit value is given. To use the generated default value, either specify the DEFAULT keyword when inserting into the identity column or leave the identity column out of the INSERT column list.

In contrast to GENERATED ALWAYS identity columns, with a GENERATED BY DEFAULT column you can specify an identity value to use instead of the generated default value. To specify a value, include it in the INSERT statement.

For example, consider a table created using the statement:

```pre
create table greetings (i int generated by default as identity, ch char(50)); 
```

The following statement specifies the value “1” for the identity column:

```pre
insert into greetings values (1, 'hi'); 
```
These statements both use generated default values:

```pre
insert into greetings values (DEFAULT, 'hello');
insert into greetings(ch) values ('bye');
```

Although the automatically-generated values in a GENERATED BY DEFAULT identity column are unique, a GENERATED BY DEFAULT column does not guarantee unique identity values for all rows in the table. For example, in the above statements, the rows containing “hi” and “hello” both have an identity value of “1.” This occurs because the generated column starts at “1” and the user-specified value was also “1.”

To avoid duplicating identity values (for example, during an import operation), you can use the START WITH clause to specify the first identity value that SnappyData should assign and increment. Or, you can use a primary key or a unique constraint on the GENERATED BY DEFAULT identity column to check for and disallow duplicates.

By default, the initial value of a GENERATED BY DEFAULT identity column is 1, and the value is incremented by 1 for each INSERT. Use the optional START WITH clause to specify a new initial value. Use the optional INCREMENT BY clause to change the increment value used during each INSERT.

**Related Topics**</br>

* [DROP TABLE](drop-table.md)

* [DELETE TABLE](delete.md)

* [SHOW TABLES](../interactive_commands/show.md#tables)

* [TRUNCATE TABLE](truncate-table.md)
