# How to Import Data from Hive Table into SnappyData Table

**Option 1** 

If Hive tables have data stored in Apache Parquet format or Optimized Row Columnar (ORC) format the data can be copied directly into SnappyData tables.

For example,
```no-highlight
create external table Hive_External_Table_Name using parquet options(path path-to-parquet-or-orc)
create table Snappy_Table_Name using column as (select * from Hive_External_Table_Name)
```
For more information on creating an external table, refer to [CREATE EXTERNAL TABLE](../reference/sql_reference/create-external-table.md)

**Option 2**

Take the RDD[Row] from Dataset of Hive Table and insert it into column table.

For example,
```no-highlight
val ds = spark.table("Hive_Table_Name")
val df = snappy.createDataFrame(ds.rdd, ds.schema)
df.write.format("column").saveAsTable("Snappy_Table_Name")
```

In above example, 'spark' is of type SparkSession and 'snappy' is of type SnappySession.
