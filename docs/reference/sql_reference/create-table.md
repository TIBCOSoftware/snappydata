# CREATE TABLE

**To Create Row/Column Table:**

```
CREATE TABLE [IF NOT EXISTS] table_name {
    ( column-definition	[ , column-definition  ] * )
	}
    USING row | column 
    OPTIONS (
    COLOCATE_WITH 'table-name',  // Default none
    PARTITION_BY 'column-name', // If not specified it will be a replicated table.
    BUCKETS  'num-partitions', // Default 113. Must be an integer.
    REDUNDANCY        'num-of-copies' , // Must be an integer
    EVICTION_BY 'LRUMEMSIZE integer-constant | LRUCOUNT interger-constant | LRUHEAPPERCENT',
    PERSISTENCE  'ASYNCHRONOUS | ASYNC | SYNCHRONOUS | SYNC | NONE’,
    DISKSTORE 'DISKSTORE_NAME', //empty string maps to default diskstore
    OVERFLOW 'true | false', // specifies the action to be executed upon eviction event
    EXPIRE 'time_to_live_in_seconds',
    COLUMN_BATCH_SIZE 'column-batch-size-in-bytes', // Must be an integer. Only for column table.
    COLUMN_MAX_DELTA_ROWS 'number-of-rows-in-each-bucket', // Must be an integer. Only for column table.
	)
	[AS select_statement];
```

Refer to these sections for more information on [Creating Sample Table](create-sample-table.md), [Creating External Table](create-external-table.md), [Creating Temporary Table](create-temporary-table.md), [Creating Stream Table](create-stream-table.md).

The column definition defines the name of a column and its data type.

<a id="column-definition"></a>
`column-definition` (for Column Table)

```
column-definition: column-name column-data-type [NOT NULL]

column-name: 'unique column name'
```
<a id="row-definition"></a>
`column-definition` (for Row Table)

```
column-definition: column-definition-for-row-table | table-constraint

column-definition-for-row-table: column-name column-data-type [ column-constraint ] *
    [ [ WITH ] DEFAULT { constant-expression | NULL } 
      | [ GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY
          [ ( START WITH start-value-as-integer [, INCREMENT BY step-value-as-integer ] ) ] ] ]
    [ column-constraint ] *
```
Refer to the [identity](#id-columns) section for more information on GENERATED.</br>
Refer to the [constraint](#constraint) section for more information on table-constraint and column-constraint.

`column-data-type`
```
column-data-type: 
	STRING | 
	INTEGER | 
	INT | 
	BIGINT | 
	LONG |  
	DOUBLE |  
	DECIMAL | 
	NUMERIC | 
	DATE | 
	TIMESTAMP | 
	FLOAT | 
	REAL | 
	BOOLEAN | 
	CLOB | 
	BLOB | 
	BINARY | 
	VARBINARY | 
	SMALLINT | 
	SHORT | 
	TINYINT | 
	BYTE | 
```
Column tables can also use ARRAY, MAP and STRUCT types.</br>
Decimal and numeric has default precision of 38 and scale of 18.</br>
In this release, LONG is supported only for column tables. It is recommended to use BEGIN fo row tables instead.

<a id="ddl"></a>
<a id="colocate-with"></a>
`COLOCATE_WITH`</br>
The COLOCATE_WITH clause specifies a partitioned table to colocate with. The referenced table must already exist. 
<a id="partition-by"></a>
`PARTITION_BY`</br>
Use the PARTITION_BY {COLUMN} clause to provide a set of column names that determines the partitioning. </br>If not specified, it is a replicated table.</br> Column and row tables support hash partitioning on one or more columns. These are specified as comma-separated column names in the PARTITION_BY option of the CREATE TABLE DDL or createTable API. The hashing scheme follows the Spark Catalyst Hash Partitioning to minimize shuffles in joins. If no PARTITION_BY option is specified for a column table, then, the table is still partitioned internally on a generated scheme.</br> The default number of storage partitions (BUCKETS) is 113 in cluster mode for column and row tables, and 11 in local mode for column and partitioned row tables. This can be changed using the BUCKETS option in CREATE TABLE DDL or createTable API.

<a id="buckets"></a>
`BUCKETS` </br>
The optional BUCKETS attribute specifies the fixed number of "buckets" to use for the partitioned row or column tables. Each data server JVM manages one or more buckets. A bucket is a container of data and is the smallest unit of partitioning and migration in the system. For instance, in a cluster of 5 nodes and bucket count of 25 would result in 5 buckets on each node. But, if you configured the reverse - 25 nodes and a bucket count of 5, only 5 data servers will host all the data for this table. If not specified, the number of buckets defaults to 113. See [best practices](/best_practices/optimizing_query_latency.md#partition-scheme) for more information.

<a id="redundancy"></a>
`REDUNDANCY`</br>
Use the REDUNDANCY clause to specify the number of redundant copies that should be maintained for each partition, to ensure that the partitioned table is highly available even if members fail. It is important to note that a redundancy of '1' implies two physical copies of data. By default, REDUNDANCY is set to 0 (zero). See [best practices](/best_practices/optimizing_query_latency.md#redundancy) for more information.

<a id="eviction-by"></a>
`EVICTION_BY`</br>
Use the EVICTION_BY clause to evict rows automatically from the in-memory table based on different criteria. You can use this clause to create an overflow table where evicted rows are written to a local SnappyStore disk store. It is important to note that all column tables (expected to host larger data sets) overflow to disk, by default. See [best practices](/best_practices/optimizing_query_latency.md#overflow) for more information. The value for this parameter is set in MB.

!!!Note:
	EVICTION_BY is not supported for replicated tables.

<a id="persistence"></a>
`PERSISTENCE`</br>
When you specify the PERSISTENCE keyword, SnappyData persists the in-memory table data to a local SnappyData disk store configuration. SnappyStore automatically restores the persisted table data to memory when you restart the member. 

!!! Note:

   	* By default, both row and column tables are persistent.

   	* The option `PERSISTENT` has been deprecated as of SnappyData 0.9. Although it does work, it is recommended to use `PERSISTENCE` instead.

<a id="diskstore"></a>
`DISKSTORE`</br>
The disk directories where you want to persist the table data. By default, SnappyData creates a "default" disk store on each member node. You can use this option to control the location where data will be stored. For instance, you may decide to use a network file system or specify multiple disk mount points to uniformly scatter the data across disks. For more information, [refer to CREATE DISKSTORE](create-diskstore.md).


<a id="overflow"></a>
`OVERFLOW`</br> 
Use the OVERFLOW clause to specify the action to be taken upon the eviction event. For persistent tables, setting this to 'true' overflows the table evicted rows to disk based on the EVICTION_BY criteria. Setting this to 'false' causes the evicted rows to be destroyed in case of eviction event.
!!! Note: 
	The tables are evicted to disk by default.

<a id="expire"></a>
`EXPIRE`</br>
Use the EXPIRE clause with tables to control the SnappyStore memory usage. It expires the rows after configured `time_to_live_in_seconds`.

<a id="column-batch-size"></a>
`COLUMN_BATCH_SIZE`</br>
The default size of blocks to use for storage in the SnappyData column store. When inserting data into the column storage this is the unit (in bytes) that is used to split the data into chunks for efficient storage and retrieval. The default value is 25165824 (24M)
   
<a id="column-max-delta-rows"></a>
`COLUMN_MAX_DELTA_ROWS`</br>
The maximum number of rows that can be in the delta buffer of a column table for each bucket, before it is flushed into the column store. Although the size of column batches is limited by COLUMN_BATCH_SIZE (and thus limits the size of row buffer for each bucket as well), this property allows a lower limit on the number of rows for better scan performance. The default value is 10000. 

!!! Note
	The following corresponding SQLConf properties for `COLUMN_BATCH_SIZE` and `COLUMN_MAX_DELTA_ROWS` are set if the table creation is done in that session (and the properties have not been explicitly specified in the DDL): 

	* `snappydata.column.batchSize` - Explicit batch size for this session for bulk insert operations. If a table is created in the session without any explicit `COLUMN_BATCH_SIZE` specification, then this is inherited for that table property. 
	* `snappydata.column.maxDeltaRows` - The maximum limit on rows in the delta buffer for each bucket of column table in this session. If a table is created in the session without any explicit COLUMN_MAX_DELTA_ROWS specification, then this is inherited for that table property.

Tables created using the standard SQL syntax without any of SnappyData specific extensions are created as row-oriented replicated tables. Thus, each data server node in the cluster hosts a consistent replica of the table. All tables are also registered in the Spark catalog and hence visible as DataFrames.

For example, `create table if not exists Table1 (a int)` is equivalent to `create table if not exists Table1 (a int) using row`.

## Examples

### Example: Column Table Partitioned on a Single Column
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

### Example: Column Table Partitioned with 10 Buckets and Persistence Enabled
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
    USING COLUMN OPTIONS (BUCKETS '10', PARTITION_BY 'C_CUSTKEY', PERSISTENCE 'SYNCHRONOUS');
```

### Example: Replicated, Persistent Row Table
```
snappy>CREATE TABLE SUPPLIER ( 
      S_SUPPKEY INTEGER NOT NULL PRIMARY KEY, 
      S_NAME STRING NOT NULL, 
      S_ADDRESS STRING NOT NULL, 
      S_NATIONKEY INTEGER NOT NULL, 
      S_PHONE STRING NOT NULL, 
      S_ACCTBAL DECIMAL(15, 2) NOT NULL,
      S_COMMENT STRING NOT NULL)
      USING ROW OPTIONS (PERSISTENCE 'ASYNCHRONOUS');
```

### Example: Row Table Partitioned with 10 Buckets and Overflow Enabled
```
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

```
CREATE TABLE CUSTOMER_STAGING USING COLUMN OPTIONS (PARTITION_BY 'C_CUSTKEY') AS SELECT * FROM CUSTOMER ;
```

With this alternate form of the CREATE TABLE statement, you specify the column names and/or the column data types with a query. The columns in the query result are used as a model for creating the columns in the new table.

If no column names are specified for the new table, then all the columns in the result of the query expression are used to create same-named columns in the new table, of the corresponding data type(s). If one or more column names are specified for the new table, the same number of columns must be present in the result of the query expression; the data types of those columns are used for the corresponding columns of the new table.

Note that only the column names and datatypes from the queried table are used when creating the new table. Additional settings in the queried table, such as partitioning, replication, and persistence, are not duplicated. You can optionally specify partitioning, replication, and persistence configuration settings for the new table and those settings need not match the settings of the queried table.

### Example: Create Table using Spark DataFrame API

For information on using the Apache Spark API, refer to [Using the Spark DataFrame API](../../aqp.md#using-the-spark-dataframe-api).

<a id="constraint"></a>
### Constraint (only for Row Tables)
A CONSTRAINT clause is an optional part of a CREATE TABLE <!--or ALTER TABLE--> statement that defines a rule to which table data must conform.

<!--!!! Note: 
	Within the scope of a transaction, SnappyData automatically initiates a rollback if it encounters a constraint violation. Errors that occur while parsing queries (such as syntax errors) or while binding parameters in a SQL statement *do not* cause a rollback.-->
There are two types of constraints:</br>
**Column-level constraints**: Refer to a single column in the table and do not specify a column name (except check constraints). They refer to the column that they follow.</br>
**Table-level constraints**: Refer to one or more columns in the table. Table-level constraints specify the names of the columns to which they apply. Table-level CHECK constraints can refer to 0 or more columns in the table.

Column and table constraints include:

* NOT NULL— Specifies that a column cannot hold NULL values (constraints of this type are not nameable).

* PRIMARY KEY— Specifies a column (or multiple columns if specified in a table constraint) that uniquely identifies a row in the table. The identified columns must be defined as NOT NULL.

* UNIQUE— Specifies that values in the column must be unique. NULL values are not allowed.

* FOREIGN KEY— Specifies that the values in the columns must correspond to values in referenced primary key or unique columns or that they are NULL. </br>If the foreign key consists of multiple columns and any column is NULL, then the whole key is considered NULL. SnappyData permits the insert no matter what is in the non-null columns.
<!-- SnappyData supports only the ON DELETE RESTRICT clause for foreign key references, and can be optionally specified that way. SnappyData checks dependent tables for foreign key constraints. If any row in a dependent table violates a foreign key constraint, the transaction is rolled back and an exception is thrown. (SnappyData does not support cascade deletion.) -->
<!--	!!! Note:
		SnappyData implicitly creates an index on child table columns that define a foreign key reference. If you attempt to create an index on that same columns manually, you will receive a warning to indicate that the index is a duplicate of an existing index.-->
* CHECK— Specifies rules for values in a column, or specifies a wide range of rules for values when included as a table constraint. The CHECK constraint has the same format and restrictions for column and table constraints.
Column constraints and table constraints have the same function; the difference is where you specify them. Table constraints allow you to specify more than one column in a PRIMARY KEY, UNIQUE, CHECK, or FOREIGN KEY constraint definition. 

Column-level constraints (except for check constraints) refer to only one column.
If you do not specify a name for a column or table constraint, then SnappyData generates a unique name.

<a id="id-columns"></a>
### Identity Columns (only for Row Tables)
<a id="generate"></a>
SnappyData supports both GENERATED ALWAYS and GENERATED BY DEFAULT identity columns only for BIGINT and INTEGER data types. The START WITH and INCREMENT BY clauses are supported only for GENERATED BY DEFAULT identity columns. <!-- Creating an identity column does not create an index on the column.-->

<!--
!!! Note: 
	When you use the SnappyData DBSynchronizer implementation to synchronize tables that have an identity column, by default DBSynchronizer does not apply the generated identity column value to the synchronized database. DBSynchronizer expects that the back-end database table also has an identity column, and that the database generates its own identity value. If you would prefer that DBSynchronizer apply the same SnappyData-generated identity column value to the underlying database, then set `skipIdentityColumns=false` in the DBSynchronizer properties file. See Restrictions and Limitations for more information about DBSynchronizer limitations.
-->
For a GENERATED ALWAYS identity column, SnappyData increments the default value on every insertion, and stores the incremented value in the column. You cannot insert a value directly into a GENERATED ALWAYS identity column, and you cannot update a value in a GENERATED ALWAYS identity column. Instead, you must either specify the DEFAULT keyword when inserting data into the table or you must leave the identity column out of the insertion column list.

Consider a table with the following column definition:

```
create table greetings (i int generated always as identity, ch char(50)) using row;
```

You can insert rows into the table using either the DEFAULT keyword or by omitting the identity column from the INSERT statement:

```
insert into greetings values (DEFAULT, 'hello');
```

```
insert into greetings(ch) values ('hi');
```
The values that SnappyData automatically generates for a GENERATED ALWAYS identity column are unique.

For a GENERATED BY DEFAULT identity column, SnappyData increments and uses a default value for an INSERT only when no explicit value is given. To use the generated default value, either specify the DEFAULT keyword when inserting into the identity column or leave the identity column out of the INSERT column list.

In contrast to GENERATED ALWAYS identity columns, with a GENERATED BY DEFAULT column you can specify an identity value to use instead of the generated default value. To specify a value, include it in the INSERT statement.

For example, consider a table created using the statement:

```
create table greetings (i int generated by default as identity, ch char(50)); 
```

The following statement specifies the value “1” for the identity column:

```
insert into greetings values (1, 'hi'); 
```
These statements both use generated default values:

```
insert into greetings values (DEFAULT, 'hello');
insert into greetings(ch) values ('bye');
```

Although the automatically-generated values in a GENERATED BY DEFAULT identity column are unique, a GENERATED BY DEFAULT column does not guarantee unique identity values for all rows in the table. For example, in the above statements, the rows containing “hi” and “hello” both have an identity value of “1.” This occurs because the generated column starts at “1” and the user-specified value was also “1.”

To avoid duplicating identity values (for example, during an import operation), you can use the START WITH clause to specify the first identity value that SnappyData should assign and increment. Or, you can use a primary key or a unique constraint on the GENERATED BY DEFAULT identity column to check for and disallow duplicates.

By default, the initial value of a GENERATED BY DEFAULT identity column is 1, and the value is incremented by 1 for each INSERT. Use the optional START WITH clause to specify a new initial value. Use the optional INCREMENT BY clause to change the increment value used during each INSERT.
