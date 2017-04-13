# CREATE SAMPLE TABLE

## SYNTAX

**To Create Sample Table:**
```
CREATE TABLE [IF NOT EXISTS] table_name {
    ( { column-definition | table-constraint }
    [ , { column-definition | table-constraint } ] * )
}
USING column_sample
OPTIONS (
COLOCATE_WITH 'string-constant',  // Default none
PARTITION_BY 'PRIMARY KEY | string-constant', // If not specified it will be a replicated table.
BUCKETS  'string-constant', // Default 113. Must be an integer.
REDUNDANCY        'string-constant' , // Must be an integer
EVICTION_BY ‘LRUMEMSIZE integer-constant | LRUCOUNT interger-constant | LRUHEAPPERCENT',
PERSISTENT  ‘ASYNCHRONOUS | SYNCHRONOUS’,
DISKSTORE 'string-constant', //empty string maps to default diskstore
OVERFLOW 'true | false', // specifies the action to be executed upon eviction event
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



## Example: 

## Example: Sample Table 

### Step 1: Create a Base Table

```
 snappy>CREATE TABLE CUSTOMER_BASE ( 
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

### Step 2: Create a Sample Table Using `CUSTOMER_BASE` Table

```
	CREATE TABLE CUSTOMER_SAMPLE <column details> 
	USING COLUMN_SAMPLE OPTIONS (qcs 'C_NATIONKEY',fraction '0.05', 
	strataReservoirSize '50', baseTable 'CUSTOMER_BASE')
	// In this case, sample table 'sampleTableName' is partitioned by column 'column_name_a', has 7 buckets and 1 redundancy.
```