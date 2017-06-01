# CREATE TEMPORARY TABLE

## SYNTAX

```
CREATE TEMPORARY TABLE [IF NOT EXISTS] [schema_name.]table_name
    [(col_name1 col_type1, ...)]
    USING datasource
    [OPTIONS (key1=val1, key2=val2, ...)]
    [AS select_statement]
```
//Jags>> The Create table syntax page should include 'external and temporary' in the syntax. You can then reference this page for details.

//Jags>> the (col_name col_type) syntax should mimic the 'create table' syntax. where it says 'column_constraint' I think. 

//Jags>> Why are mixing Temporary tables with external tables. Makes sense to have section on external. Temp stuff should be in the main 'create table' reference.

**TEMPORARY**
Temporary tables are scoped to SQL connection or the Snappy Spark session that creates it. This table will not appear like the system catalog nor visible to other connections or sessions. 

**USING <data source>**
Specify the file format to use for this table. The data source may be one of TEXT, CSV, JSON, JDBC, PARQUET, ORC, and LIBSVM, or a fully qualified class name of a custom implementation of org.apache.spark.sql.sources.DataSourceRegister. Note that most of the prominent datastores provide an implementation of 'DataSource' and accessible as a table. For instance, you can use the Cassandra spark package to create external tables pointing to Cassandra tables and directly run queries on them. You can mix any external table and SnappyData managed tables in your queries. 

**AS <select_statement>**
Populate the table with input data from the select statement. 

//Jags>> does this really work? this would imply external tables are writable. I don't think so. 

## Example 

**Create a Temporary Table**

```
snappy> CREATE TEMPORARY TABLE STAGING_AIRLINEREF USING parquet OPTIONS(path '../../quickstart/data/airportcodeParquetData');

snappy> CREATE TEMPORARY TABLE STAGING_AIRLINE_TEMP (ArrDelay int, DepDelay int, Origin string, Dest String) AS SELECT ArrDelay, DepDelay, Origin, Dest FROM STAGING_AIRLINE;
```
