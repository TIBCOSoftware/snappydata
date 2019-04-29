# How to Import Data from Hive Table into TIBCO ComputeDB Table

**Option 1** 

If Hive tables have data stored in Apache Parquet format or Optimized Row Columnar (ORC) format the data can be copied directly into TIBCO ComputeDB tables.

For example,
```pre
CREATE EXTERNAL TABLE <hive_external_table_name> USING parquet OPTIONS(path path-to-parquet-or-orc)

CREATE TABLE <table_name> USING COLUMN AS (select * from hive_external_table_name)
```
For more information on creating an external table, refer to [CREATE EXTERNAL TABLE](../reference/sql_reference/create-external-table.md).

**Option 2**

Take the RDD[Row] from Dataset of Hive Table and insert it into column table.

For example,
```pre
val ds = spark.table("Hive_Table_Name")
val df = snappy.createDataFrame(ds.rdd, ds.schema)
df.write.format("column").saveAsTable("Snappy_Table_Name")
```

In above example, 'spark' is of type SparkSession and 'snappy' is of type SnappySession.
