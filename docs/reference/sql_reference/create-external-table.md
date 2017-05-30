# CREATE EXTERNAL TABLE

## SYNTAX

```
CREATE EXTERNAL  TABLE [IF NOT EXISTS] [schema_name.]table_name
    [(col_name1 col_type1, ...)]
    USING datasource
    [OPTIONS (key1=val1, key2=val2, ...)]
    [AS select_statement]
```
//Jags>> The Create table syntax page should include 'external and temporary' in the syntax. You can then reference this page for details.

//Jags>> the (col_name col_type) syntax should mimic the 'create table' syntax. where it says 'column_constraint' I think. 

//Jags>> Why are mixing Temporary tables with external tables. Makes sense to have section on external. Temp stuff should be in the main 'create table' reference. 

**EXTERNAL**
External tables point to external data sources. SnappyData supports all the data sources supported by Spark. You should use external tables to load data in parallel from any of the external sources. See list below. The table definition is persisted in the catalog and visible across all sessions. 

**USING <data source>**
Specify the file format to use for this table. The data source may be one of TEXT, CSV, JSON, JDBC, PARQUET, ORC, and LIBSVM, or a fully qualified class name of a custom implementation of org.apache.spark.sql.sources.DataSourceRegister. Note that most of the prominent datastores provide an implementation of 'DataSource' and accessible as a table. For instance, you can use the Cassandra spark package to create external tables pointing to Cassandra tables and directly run queries on them. You can mix any external table and SnappyData managed tables in your queries. 

**AS <select_statement>**
Populate the table with input data from the select statement. 

//Jags>> does this really work? this would imply external tables are writable. I don't think so. 

## Example 

**Create an external table using PARQUET data source**

```
snappy> CREATE EXTERNAL TABLE STAGING_AIRLINE USING parquet OPTIONS(path '../../quickstart/data/airlineParquetData');
```

//Jags>> really need more examples ... show example using CSV with several of the important options. Do we have a Data loading section? very much desired. 