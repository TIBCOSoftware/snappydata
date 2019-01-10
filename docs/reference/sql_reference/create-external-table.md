# CREATE EXTERNAL TABLE

```pre
CREATE EXTERNAL TABLE [IF NOT EXISTS] [schema_name.]table_name
    [( column-definition	[ , column-definition  ] * )]
    USING datasource
     [OPTIONS (key1 val1, key2 val2, ...)]
```

For more information on column-definition, refer to [Column Definition For Column Table](create-table.md#column-definition).

Refer to these sections for more information on [Creating Table](create-table.md), [Creating Sample Table](create-sample-table.md), [Creating Temporary Table](create-temporary-table.md) and [Creating Stream Table](create-stream-table.md).

**EXTERNAL**

External tables point to external data sources. SnappyData supports all the data sources supported by Spark. You should use external tables to load data in parallel from any of the external sources. The table definition is persisted in the catalog and visible across all sessions. 

**USING <_data source_>**

Specify the file format to use for this table. The data source may be one of TEXT, CSV, JSON, JDBC, PARQUET, ORC, and LIBSVM, or a fully qualified class name of a custom implementation of org.apache.spark.sql.sources.DataSourceRegister. </br>Note that most of the prominent datastores provide an implementation of 'DataSource' and accessible as a table. For instance, you can use the Cassandra spark package to create external tables pointing to Cassandra tables and directly run queries on them. You can mix any external table and SnappyData managed tables in your queries. 

## Example 

Create an external table using PARQUET data source

```pre
snappy> CREATE EXTERNAL TABLE STAGING_AIRLINE USING parquet OPTIONS(path '../../quickstart/data/airlineParquetData');
```

Create an external table using CSV data source

```pre
CREATE EXTERNAL TABLE IF NOT EXISTS CUSTOMER_STAGING USING csv OPTIONS(path '../../quickstart/src/main/resources/customer.csv');
```

```pre
CREATE EXTERNAL TABLE CUSTOMER_STAGING_1 (C_CUSTKEY INTEGER NOT NULL, C_NAME VARCHAR(25) NOT NULL, 
C_ADDRESS VARCHAR(40) NOT NULL, C_NATIONKEY INTEGER NOT NULL, C_PHONE VARCHAR(15) NOT NULL, 
C_ACCTBAL DECIMAL(15,2) NOT NULL, C_MKTSEGMENT VARCHAR(10) NOT NULL, C_COMMENT VARCHAR(117) NOT NULL) 
USING csv OPTIONS (path '../../quickstart/src/main/resources/customer.csv');
```

You can also load data from AWS S3, as given in the example below:

```pre
CREATE EXTERNAL TABLE NYCTAXI USING parquet OPTIONS(path 's3a://<AWS_SECRET_KEY>:<AWS_SECRET_ID>@<folder>/<data>');
```

**Related Topics**

* [DROP EXTERNAL TABLE](drop-table.md)
