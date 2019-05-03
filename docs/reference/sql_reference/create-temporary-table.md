# CREATE TEMPORARY TABLE

```pre
CREATE TEMPORARY TABLE table_name
    USING datasource
    [AS select_statement];
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

```pre
snappy> CREATE TEMPORARY TABLE STAGING_AIRLINEREF USING parquet OPTIONS(path '../../quickstart/data/airportcodeParquetData');
```

```pre
snappy> CREATE TEMPORARY TABLE STAGING_AIRLINE_TEMP2 AS SELECT CODE, DESCRIPTION FROM STAGING_AIRLINEREF;
```

!!! Note
    When creating a temporary table, the TIBCO ComputeDB catalog is not referred, which means, a temporary table with the same name as that of an existing TIBCO ComputeDB table can be created. Two tables with the same name lead to ambiguity during query execution and can either cause the query to fail or return wrong results. </br>Ensure that you create temporary tables with a unique name.
    
## CREATE GLOBAL TEMPORARY TABLE

```pre
snappy> CREATE GLOBAL TEMPORARY TABLE [global-temporary-table-name] USING PARQUET OPTIONS(path 'path-to-parquet');

snappy> CREATE GLOBAL TEMPORARY TABLE [global-temporary-table-name] AS SELECT [column-name], [column-name] FROM [table-name];
```

### Description
Specifies a table definition that is visible to all sessions. Temporary table data is visible only to the session that inserts the data into the table.

### Examples

```pre
snappy> CREATE GLOBAL TEMPORARY TABLE STAGING_AIRLINEREF1 USING parquet OPTIONS(path '../../quickstart/data/airportcodeParquetData');

snappy> CREATE GLOBAL TEMPORARY TABLE STAGING_AIRLINE2 AS SELECT CODE, DESCRIPTION FROM STAGING_AIRLINEREF;
```
!!! Note
	Temporary views/tables are scoped to SQL connection or the Snappy Spark session that creates it. VIEW or TABLE are synonyms in this context with former being the preferred usage. This table does not appear in the system catalog nor visible to other connections or sessions.
