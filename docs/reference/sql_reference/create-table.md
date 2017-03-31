# CREATE TABLE

## SYNTAX
```
CREATE TABLE [IF NOT EXISTS] table_name {
    ( { column-definition | table-constraint }
    [ , { column-definition | table-constraint } ] * )
| 
    [ ( column-name [, column-name ] * ) ]
    AS query-expression
    WITH NO DATA
}

USING row | column
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
)
[AS select_statement];
```

## Description

Tables contain columns and constraints, rules to which data must conform. Table-level constraints specify a column or columns. Columns have a data type and can specify column constraints (column-level constraints). The syntax of CREATE TABLE is extended to give properties to the tables that are specific to RowStore.

The CREATE TABLE statement has two variants depending on whether you are specifying the column definitions and constraints (CREATE TABLE), or whether you are modeling the columns after the results of a query expression (CREATE TABLE…AS…).

## Example Column Table
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
        
## Example Row Table
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
          
## CREATE TABLE … AS …
With the alternate form of the CREATE TABLE statement, you specify the column names and/or the column data types with a query. The columns in the query result are used as a model for creating the columns in the new table.

If no column names are specified for the new table, then all the columns in the result of the query expression are used to create same-named columns in the new table, of the corresponding data type(s). If one or more column names are specified for the new table, the same number of columns must be present in the result of the query expression; the data types of those columns are used for the corresponding columns of the new table.

Note that only the column names and datatypes from the queried table are used when creating the new table. Additional settings in the queried table, such as partitioning, replication, and persistence, are not duplicated. You can optionally specify partitioning, replication, and persistence configuration settings for the new table, and those settings need not match the settings of the queried table.

The WITH NO DATA clause specifies that the data rows that result from evaluating the query expression are not used; only the names and data types of the columns in the query result are used. The WITH NO DATA clause is required.

## Example

TO BE DONE