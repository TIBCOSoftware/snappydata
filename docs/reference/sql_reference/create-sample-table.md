# CREATE SAMPLE TABLE

When creating a base table, if you have applied the partition by clause, the clause is also applied to the sample table. The sample table also inherits the number of buckets, redundancy and persistence properties from the base table.
For sample tables, the overflow property is set to **False** by default. For column tables the default value is **True**.

## SYNTAX

**Mode 1 - To Create Row/Column Table:**
```
    CREATE TABLE [IF NOT EXISTS] table_name {
        ( { column-definition | table-constraint }
        [ , { column-definition | table-constraint } ] * )
    }
    USING column_sample
    OPTIONS (
    COLOCATE_WITH 'string-constant',  // Default none
    BUCKETS  'string-constant', // Default 113. Must be an integer.
    REDUNDANCY        'string-constant' , // Must be an integer
    EVICTION_BY ‘LRUMEMSIZE integer-constant | LRUCOUNT interger-constant | LRUHEAPPERCENT',
    PERSISTENT  ‘ASYNCHRONOUS | SYNCHRONOUS’,
    DISKSTORE 'string-constant', //empty string maps to default diskstore
    OVERFLOW 'true | false', // specifies the action to be executed upon eviction event
    EXPIRE ‘TIMETOLIVE in seconds',
    )
    [AS select_statement];
```    

** Mode 2 - To Create Sample Table:**
```
    CREATE SAMPLE TABLE table_name ON base_table_name
    OPTIONS (
    COLOCATE_WITH 'string-constant',  // Default none
    BUCKETS  'string-constant', // Default 113. Must be an integer.
    REDUNDANCY        'string-constant' , // Must be an integer
    EVICTION_BY ‘LRUMEMSIZE integer-constant | LRUCOUNT interger-constant | LRUHEAPPERCENT',
    PERSISTENT  ‘ASYNCHRONOUS | SYNCHRONOUS’,
    DISKSTORE 'string-constant', //empty string maps to default diskstore
    OVERFLOW 'true | false', // specifies the action to be executed upon eviction event
    EXPIRE ‘TIMETOLIVE in seconds',
    QCS 'string-constant', // column-name [, column-name ] *
    FRACTION 'string-constant',  //Must be a double
    STRATARESERVOIRSIZE 'string-constant',  // Default 50 Must be an integer.
    )
    AS select_statement
```
When creating a base table, if you have applied the partition by clause, the clause is also applied to the sample table. The sample table also inherits the number of buckets, redundancy and persistence properties from the base table.
For sample tables, the overflow property is set to False by default. (For column tables the default value is True).

## Description
 * QCS: Query Column Set. These columns are used for startification in startified sampling. 

 * FRACTION: This represents the fraction of the full population (base table) that is managed in the sample. 

 * STRATARESERVOIRSIZE: Intial capacity of each strata.

 * baseTable : Table on which sampling is done.

Refer to [create sample tables](concepts/sde/stratified_sampling/#create-sample-tables) for more information on creating sample tables on datasets that can be sourced from any source supported in Spark/SnappyData.

## Example: Create a Sample Table 

### Step 1: Create a Base Table

```
 snappy>CREATE TABLE CUSTOMER_SAMPLE ( 
        C_CUSTKEY     INTEGER NOT NULL,
        C_NAME        VARCHAR(25) NOT NULL,
        C_ADDRESS     VARCHAR(40) NOT NULL,
        C_NATIONKEY   INTEGER NOT NULL,
        C_PHONE       VARCHAR(15) NOT NULL,
        C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
        C_MKTSEGMENT  VARCHAR(10) NOT NULL,
        C_COMMENT     VARCHAR(117) NOT NULL)
    USING COLUMN_SAMPLE OPTIONS (qcs 'C_NATIONKEY',fraction '0.05', 
    strataReservoirSize '50', baseTable 'CUSTOMER_BASE')
```

### Step 2: Create a Sample Table On `CUSTOMER_BASE` Table

```
    CREATE SAMPLE TABLE CUSTOMER_SAMPLE on CUSTOMER_BASE     
    OPTIONS (qcs 'C_NATIONKEY',fraction '0.05', 
    strataReservoirSize '50') AS (SELECT * FROM CUSTOMER_BASE);    
```

## Example: Stratified sample Table  

<mark>Need Example with explanation </mark>
```
CREATE TABLE CUSTOMER_SAMPLE ( 
        C_CUSTKEY     INTEGER NOT NULL,
        C_NAME        VARCHAR(25) NOT NULL,
        C_ADDRESS     VARCHAR(40) NOT NULL,
        C_NATIONKEY   INTEGER NOT NULL,
        C_PHONE       VARCHAR(15) NOT NULL,
        C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
        C_MKTSEGMENT  VARCHAR(10) NOT NULL,
        C_COMMENT     VARCHAR(117) NOT NULL)
    USING COLUMN_SAMPLE OPTIONS (qcs 'C_NATIONKEY',fraction '0.05', 
    strataReservoirSize '50', baseTable 'CUSTOMER_BASE')
```