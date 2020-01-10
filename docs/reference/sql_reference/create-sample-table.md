# CREATE SAMPLE TABLE

**Mode 1**

```pre
CREATE TABLE [IF NOT EXISTS] table_name 
    ( column-definition	[ , column-definition  ] * )
    USING column_sample
    OPTIONS (
    baseTable 'baseTableName',
    BUCKETS  'num-partitions', // Default 128. Must be an integer.
    REDUNDANCY        'num-of-copies' , // Must be an integer
    EVICTION_BY ‘LRUMEMSIZE integer-constant | LRUCOUNT interger-constant | LRUHEAPPERCENT',
    PERSISTENCE  ‘ASYNCHRONOUS | SYNCHRONOUS’,
    DISKSTORE 'diskstore-name', //empty string maps to default diskstore
    OVERFLOW 'true | false', // specifies the action to be executed upon eviction event
    EXPIRE ‘time-to-live-in-seconds',
    QCS 'column-name', // column-name [, column-name ] *
    FRACTION 'population-fraction',  //Must be a double
    STRATARESERVOIRSIZE 'strata-initial-capacity',  // Default 50 Must be an integer.
    )
    [AS select_statement];
```    

**Mode 2**

```pre
CREATE SAMPLE TABLE table_name ON base_table_name
    OPTIONS (
    COLOCATE_WITH 'table-name',  // Default none
    BUCKETS  'num-partitions', // Default 128. Must be an integer.
    REDUNDANCY        'num-redundant-copies' , // Must be an integer
    EVICTION_BY ‘LRUMEMSIZE integer-constant | LRUCOUNT interger-constant | LRUHEAPPERCENT',
    PERSISTENCE  ‘ASYNCHRONOUS | SYNCHRONOUS’,
    DISKSTORE 'diskstore-name', //empty string maps to default diskstore
    OVERFLOW 'true | false', // specifies the action to be executed upon eviction event
    EXPIRE ‘time-to-live-in-seconds',
    QCS 'column-name', // column-name [, column-name ] *
    FRACTION 'population-fraction',  //Must be a double
    STRATARESERVOIRSIZE 'strata-initial-capacity',  // Default 50 Must be an integer.
    )
    
```
For more information on column-definition, refer to [Column Definition For Column Table](create-table.md#column-definition).

When creating a base table, if you have applied the partition by clause, the clause is also applied to the sample table. The sample table also inherits the number of buckets, redundancy and persistence properties from the base table.
For sample tables, the overflow property is set to **False** by default. For column tables the default value is **True**.

Refer to these sections for more information on [Creating Table](create-table.md), [Creating External Table](create-external-table.md), [Creating Temporary Table](create-temporary-table.md) and [Creating Stream Table](create-stream-table.md).

## Description

 * **QCS**: Query Column Set. These columns are used for stratification in stratified sampling. 

 * **FRACTION**: This represents the fraction of the full population (base table) that is managed in the sample. 

 * **STRATARESERVOIRSIZE**: The initial capacity of each stratum.

 * **baseTable**: Table on which sampling is done.

## Examples: 

### Mode 1 Example

```pre
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
      strataReservoirSize '50', baseTable 'CUSTOMER_BASE');
```

### Mode 2 Example

```pre
snappy>CREATE SAMPLE TABLE CUSTOMER_SAMPLE on CUSTOMER_BASE
      OPTIONS (qcs 'C_NATIONKEY',fraction '0.05', 
      strataReservoirSize '50');
```    

!!! Note
	Refer to [create sample tables in SDE section](/../../sde/working_with_stratified_samples.md#create-sample-tables) for more information on creating sample tables on datasets that can be sourced from any source supported in Spark/TIBCO ComputeDB.
