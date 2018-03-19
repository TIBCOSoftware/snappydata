# CREATE TEMPORARY TABLE

```no-highlight
CREATE TEMPORARY TABLE [schema_name.]table_name
    ( column-definition	[ , column-definition  ] * )
    USING datasource
    [OPTIONS (key1=val1, key2=val2, ...)]
    [AS select_statement]
```

For more information on column-definition, refer to [Column Definition For Column Table](create-table.md#column-definition).

Refer to these sections for more information on [Creating Table](create-table.md), [Creating Sample Table](create-sample-table.md), [Creating External Table](create-external-table.md) and [Creating Stream Table](create-stream-table.md).

**TEMPORARY**

Temporary tables are scoped to SQL connection or the Snappy Spark session that creates it. This table does not appear in the system catalog nor visible to other connections or sessions.

**USING <data source>**

Specify the file format to use for this table. The data source may be one of TEXT, CSV, JSON, JDBC, PARQUET, ORC, and LIBSVM, or a fully qualified class name of a custom implementation of org.apache.spark.sql.sources.DataSourceRegister.

**AS <select_statement>**
Populate the table with input data from the select statement. 

## Examples

**Create a Temporary Table**

```no-highlight
snappy> CREATE TEMPORARY TABLE STAGING_AIRLINEREF USING parquet OPTIONS(path '../../quickstart/data/airportcodeParquetData');
```

```no-highlight
snappy> CREATE TEMPORARY TABLE STAGING_AIRLINE_TEMP (CODE2 string, DESCRIPTION2 String) AS SELECT CODE, DESCRIPTION FROM STAGING_AIRLINEREF;
```

```no-highlight
snappy> CREATE TEMPORARY TABLE STAGING_AIRLINE_TEMP2 AS SELECT CODE, DESCRIPTION FROM STAGING_AIRLINEREF;
```

!!! Note:
    When creating a temporary table, the SnappyData catalog is not referred, which means, a temporary table with the same name as that of an existing SnappyData table can be created. Two tables with the same name lead to ambiguity during query execution and can either cause the query to fail or return wrong results. </br>Ensure that you create temporary tables with a unique name.