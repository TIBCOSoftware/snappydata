# Getting Started with SnappyData CDC (Change Data Capture) Connector

<ent>This feature is available only in the Enterprise version of SnappyData.</ent>

The Change Data Capture (CDC) mechanism enables you to capture changed data. The functionality provides an efficient framework for tracking inserts, updates, and deletes in tables in a SQL Server database. It is used to source event streams from JDBC sources to SnappyData streaming.</br>

In this topic, we explain how SnappyData uses the JDBC streaming connector to pull changed data from SQL database and ingest it into SnappyData tables.

!!!Note:

	- Writing data into two different tables from a single table is currently not supported as the schema for incoming data frame cannot be changed. Currently, one to one correspondence between the source table and destination table is supported.

	- To capture changed data for multiple source tables, ensure that you create an instance for each source table.

	-  For every source table that needs to be tracked for changes, ensure that there is a corresponding destination table in SnappyData.


## Prerequisites

- Use a user account with the required roles and privileges to the database.

- Ensure that a SQL Server to which SnappyData CDC Connector can connect is running and available.

- You need the **snappydata-jdbc-stream-connector_2.11-0.9.jar**, which is available in the **$SNAPPY_HOME/jars** directory. </br>If you are using Maven or Gradle project to develop the streaming application, you need to publish the above jar into a local maven repository.

- To run your application in the Smart connector mode, refer to the [documentation](../affinity_modes/connector_mode.md).

The following image illustrates the data flow for change data capture:</br>
![CDC Workflow](../Images/cdc_connector.png)

## Setting up the CDC Connector

To use this functionality have to configure the database and tables, enable streaming for the source tables, configure the reader and the writer.

In this section, we discuss the steps required to set up the CDC connector and various components.

### Understanding the Program Structure

The RDB CDC connector ingests data into SnappyData from any JDBC enabled data server. We have a custom source with alias “jdbcStream” and a custom Sink with alias “snappystore”. Source has the capability to read from a JDBC source and Sink can do inserts/updates and deletes based on CDC operations

### Step 1: Set Up the Source SQL Database and Tables

Ensure that change data capture is enabled for the database and table level. Refer to your database documentation for more information on enabling CDC.

### Step 2: Enable Streaming for the Source Tables and configure the Reader



#### Enable Streaming for the Source Tables
Structured streaming and Spark’s JDBC source is used to read from the source database system.

To enable this, set a stream referring to the source table.</br>
For example:

       DataStreamReader reader = snappYSession.readStream()
          .format("jdbcStream")
          .option("spec", "org.apache.spark.sql.streaming.jdbc.SqlServerSpec")
          .option("sourceTableName", sourceTable)
          .option("maxEvents", "50000")
          .options(connectionOptions);
          
The JDBC Stream Reader options are:

- **jdbcStream**: The format in which the source data is received from the SQL server.

- **spec**: A CDC spec class name which is used to query offsets from different data sources. We have a default specification for SQLServer. You can extend the `org.apache.spark.sql.streaming.jdbc.SourceSpec` trait to provide any other implementation.

        trait SourceSpec {

          /** A fixed offset column name in the source table. */
          def offsetColumn: String

          /** An optional function to convert offset
            * to string format, if offset is not in string format.
            */
          def offsetToStrFunc: Option[String]

          /** An optional function to convert string offset to native offset format. */
          def strToOffsetFunc: Option[String]

          /** Query to find the next offset */
          def getNextOffset(tableName : String, currentOffset : String, maxEvents : Int): String
        }

	The default implementation of SQLServerSpec can be:

        class SqlServerSpec extends SourceSpec {

          override def offsetColumn: String = "__$start_lsn"

          override def offsetToStrFunc: Option[String] = Some("master.dbo.fn_varbintohexstr")

          override def strToOffsetFunc: Option[String] = Some("master.dbo.fn_cdc_hexstrtobin")

          /** make sure to convert the LSN to string and give a column alias for the
            * user defined query one is supplying. Alternatively, a procedure can be invoked
            * with $table, $currentOffset Snappy provided variables returning one string column
            * representing the next LSN string.
            */
          override def getNextOffset(tableName : String, currentOffset : String, maxEvents : Int): String =
            s"""select master.dbo.fn_varbintohexstr(max(${offsetColumn})) nextLSN
                 from (select ${offsetColumn}, sum(count(1)) over
                 (order by ${offsetColumn}) runningCount
                from $tableName where ${offsetColumn} > master.dbo.fn_cdc_hexstrtobin('$currentOffset')
                  group by ${offsetColumn}) x where runningCount <= $maxEvents"""
        }

- **sourceTableName**: The source table from which the data is read. For example, **testdatabase.cdc.dbo_customer_CT**.

- **maxEvents**: Number of rows to be read in one batch.

- **connectionOptions**: A map of key values containing different parameters to access the source database.

    | Key | Value |
    |--------|--------|
    |`driver`|JDBC driver to be used to access source database. E.g. com.microsoft.sqlserver.jdbc.SQLServerDriver|
    |`url`|JDBC connection url|
    |`user`|User name |
    |`password`|Password|
    |`databaseName`|Database name|
    |`poolImpl`|Connection pool implementation(default Hikari) Only Tomcat and Hikari are supported|
    |`maximumPoolSize`|Connection pool size ( default 10)|
    |`minimumIdle`|Minimum number of idle connection( default 5)|

#### Operating on the reader with normal Structured streaming APIs

The sample usage can be as follows:

```
Dataset<Row> ds = reader.load();
ds.filter(<filter_condition>)
```
### Step 3: Writing into SnappyData tables

To write into SnappyData tables you need to have a StreamWriter as follows:

```
ds.writeStream()
        .trigger(ProcessingTime.create(10, TimeUnit.SECONDS))
        .format("snappystore")
        .option("sink", ProcessEvents.class.getName())
        .option("tableName", tableName)
        .start();
```

#### Snappy Stream Writer options

The **sink** option is mandatory for SnappyStore sink. This option is required to give the user control of the obtained data frame. You can, however, provide any custom option when writing the streaming data to the tables.

| Key | Value |
|--------|--------|
|sink|A user-defined callback class which will get a data frame in each batch. The class must implement org.apache.spark.sql.streaming.jdbc.SnappyStreamSink interface.|

**org.apache.spark.sql.streaming.jdbc.SnappyStreamSink**

The above trait contains a single method, which user needs to implement. A user can use SnappyData mutable APIs (insert/update/delete/putInto) to maintain tables.

```pre
    def process(snappySession: SnappySession, sinkProps: Properties,
        batchId: Long, df: Dataset[Row]): Unit
```

A typical implementation would look like:

```
@Override
  public void process(SnappySession snappySession, Properties sinkProps,
      long batchId, Dataset<Row> df) {

    String snappyTable = sinkProps.getProperty("TABLENAME").toUpperCase();

    log.info("SB: Processing for " + snappyTable + " batchId " + batchId);

    df.cache();

    Dataset<Row> snappyCustomerDelete = df
        // pick only delete ops
        .filter("\"__$operation\" = 1")
        // exclude the first 5 columns and pick the columns that needs to control
        // the WHERE clause of the delete operation.
        .drop(metaColumns.toArray(new String[metaColumns.size()]));

      snappyJavaUtil(snappyCustomerDelete.write()).deleteFrom("APP." + snappyTable);

      if(batchId == 0){ // Batch ID will be always 0 when stream app starts
        // In case of 0th batchId always do a putInto , as we do not know if it is restarting from a failure.
        Dataset<Row> snappyCustomerUpsert = df
            // pick only insert/update ops
            .filter("\"__$operation\" = 4 OR \"__$operation\" = 2")
            // exclude the first 5 columns and pick the rest as columns have
            // 1-1 correspondence with snappydata customer table.
            .drop(metaColumns.toArray(new String[metaColumns.size()]));
        snappyJavaUtil(snappyCustomerUpsert.write()).putInto("APP." + snappyTable);
      } else {
        Dataset<Row> snappyCustomerUpdates = df
            // pick only insert/update ops
            .filter("\"__$operation\" = 4")
            .drop(metaColumns.toArray(new String[metaColumns.size()]));

        snappyJavaUtil(snappyCustomerUpdates.write()).update("APP." + snappyTable);

        Dataset<Row> snappyCustomerInserts = df
            // pick only insert/update ops
            .filter("\"__$operation\" = 2")
            .drop(metaColumns.toArray(new String[metaColumns.size()]));

        try{
          snappyCustomerInserts.write().insertInto("APP." + snappyTable);
        }catch (Exception e) {
          snappyJavaUtil(snappyCustomerInserts.write()).putInto("APP." + snappyTable);
        }
      }
  }
```

## Additional Information

- **Offset Management**: </br>
	One table per application is maintained for offset management (Offset table name for application = AppName_SNAPPY_JDBC_STREAM_CONTEXT). </br>
    The schema of the table is:

    |TABLENAME  |LASTOFFSET |
    |--------|--------|

    Initially, the table contains no rows. The connector inserts a row for the table with minimum offset queried from the database. On subsequent intervals, it consults this table to get the offset number. </br>
    If connector application crashes, it refers to this table on a restart to query further.

    !!! Note
        The Spark configuration property `spark.task.maxFailures` defines the number of failures before giving up on the job. Set the value of this property to 0, for connector application to prevent Spark from retrying failed inserts.
    
- **Primary Keys**: </br>
	Primary keys are not supported for column tables. You can use `key_columns` instead, to uniquely identify each row/record in a database table. </br> For more information, see [CREATE EXTERNAL TABLE](../reference/sql_reference/create-external-table.md) and [CREATE TABLE](../reference/sql_reference/create-table.md).</br>
    For example:

		CREATE EXTERNAL staging_<table_name> USING csv OPTIONS(<path_to_csv_file>)

		CREATE TABLE <table_name> USING column OPTIONS(partition_by '<column_name>', buckets '<num_partitions>',key_columns '<primary_key>') AS (SELECT * FROM from external table);
