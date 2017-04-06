# CREATE EXTERNAL TABLE OR TEMPORARY TABLE

## SYNTAX

```
CREATE TEMPORARY | EXTERNAL  TABLE [IF NOT EXISTS] [schema_name.]table_name
    [(col_name1 col_type1, ...)]
    USING datasource
    [OPTIONS (key1=val1, key2=val2, ...)]
    [AS select_statement]
```

## Description
Create a temporary or external table using a data source. If a table with the same name already exists in the database, an exception will be thrown.

**TEMPORARY**
The created table will be available only in this session and will not be persisted to the underlying metastore.

**EXTERNAL**
The created table will be available to all sessions and the entry for table will be persisted to the underlying metastore.

**IF NOT EXISTS**
If a table with the same name already exists in the database, nothing will happen.

**USING <data source>**
Specify the file format to use for this table. The data source may be one of TEXT, CSV, JSON, JDBC, PARQUET, ORC, and LIBSVM, or a fully qualified class name of a custom implementation of org.apache.spark.sql.sources.DataSourceRegister.

**AS <select_statement>**
Populate the table with input data from the select statement. 

## Example 

**Create an external table using PARQUET data source**

```
snappy> CREATE EXTERNAL TABLE STAGING_AIRLINE USING parquet OPTIONS(path '../../quickstart/data/airlineParquetData');
```

**Create a temporary table**

```
snappy> CREATE TEMPORARY TABLE STAGING_AIRLINEREF USING parquet OPTIONS(path '../../quickstart/data/airportcodeParquetData');

snappy> CREATE TEMPORARY TABLE STAGING_AIRLINE_TEMP (ArrDelay int, DepDelay int, Origin string, Dest String) AS SELECT ArrDelay, DepDelay, Origin, Dest FROM STAGING_AIRLINE;
```