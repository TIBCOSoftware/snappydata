# CREATE TABLE

## SYNTAX

**To Create Row/Column Table:**

```
CREATE TABLE [IF NOT EXISTS] table_name {
    ( { column-definition | table-constraint }
    [ , { column-definition | table-constraint } ] * )
| 
    [ ( column-name [, column-name ] * ) ]
    AS query-expression
    WITH NO DATA
}

USING row | column | column_sample
OPTIONS (
COLOCATE_WITH 'string-constant',  // Default none
PARTITION_BY 'PRIMARY KEY | string-constant', // If not specified it will be a replicated table.
BUCKETS  'string-constant', // Default 113. Must be an integer.
REDUNDANCY        'string-constant' , // Must be an integer
EVICTION_BY ‘LRUMEMSIZE integer-constant | LRUCOUNT interger-constant | LRUHEAPPERCENT',
PERSISTENT  ‘ASYNCHRONOUS | SYNCHRONOUS’, 
DISKSTORE 'string-constant', //empty string maps to default diskstore
EXPIRE ‘TIMETOLIVE in seconds',
COLUMN_BATCH_SIZE 'string-constant', // Must be an integer
COLUMN_MAX_DELTA_ROWS 'string-constant', // Must be an integer
QCS 'string-constant', // column-name [, column-name ] *
FRACTION 'string-constant',  //Must be a double 
STRATARESERVOIRSIZE 'string-constant',  // Default 50 Must be an integer.
BASETABLE 'string-constant', //base table name
)
[AS select_statement];
```

## Description

Tables contain columns and constraints, rules to which data must conform. Table-level constraints specify a column or columns. Columns have a data type and can specify column constraints (column-level constraints). The syntax of CREATE TABLE is extended to give properties to the tables that are specific to RowStore.

The CREATE TABLE statement has two variants depending on whether you are specifying the column definitions and constraints (CREATE TABLE), or whether you are modeling the columns after the results of a query expression (CREATE TABLE…AS…).

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
   

## Example: Column Table
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
        
## Example: Row Table
```
    snappy>CREATE TABLE SUPPLIER ( 
          S_SUPPKEY INTEGER NOT NULL PRIMARY KEY, 
          S_NAME STRING NOT NULL, 
          S_ADDRESS STRING NOT NULL, 
          S_NATIONKEY INTEGER NOT NULL, 
          S_PHONE STRING NOT NULL, 
          S_ACCTBAL DECIMAL(15, 2) NOT NULL,
          S_COMMENT STRING NOT NULL)
          USING ROW OPTIONS (PERSISTENT 'asynchronous');
          
          ```
          
## Example: Sample Table          


## CREATE TABLE … AS …
With the alternate form of the CREATE TABLE statement, you specify the column names and/or the column data types with a query. The columns in the query result are used as a model for creating the columns in the new table.

If no column names are specified for the new table, then all the columns in the result of the query expression are used to create same-named columns in the new table, of the corresponding data type(s). If one or more column names are specified for the new table, the same number of columns must be present in the result of the query expression; the data types of those columns are used for the corresponding columns of the new table.

Note that only the column names and datatypes from the queried table are used when creating the new table. Additional settings in the queried table, such as partitioning, replication, and persistence, are not duplicated. You can optionally specify partitioning, replication, and persistence configuration settings for the new table, and those settings need not match the settings of the queried table.

The WITH NO DATA clause specifies that the data rows that result from evaluating the query expression are not used; only the names and data types of the columns in the query result are used. The WITH NO DATA clause is required.

## Example

TO BE DONE