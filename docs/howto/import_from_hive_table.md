# How to Import Data from Hive Table into Snappydata Table

**Option 1** 

Data kept in Apache Parquet format or Optimized Row Columnar (ORC) format can be directly copied into SnappyData tables

For example,
```
create external table ext_hive_table using parquet options('path' ....) ; 
       create table t1 using column as (select * from ext_hive_table )'
```
For more information on creating an external table, refer to [CREATE EXTERNAL TABLE](../reference/sql_reference/create-table.md)

**Option 2**

Take the RDD[Row] from Dataset of Hive Table and insert it into column table

For example,
```
val ds = spark.table("Hive_Table_Name")
val df = snappy.createDataFrame(ds.rdd, ds.schema)
val df = snappy.createDataFrame(ds.rdd, ds.schema)
df.write.format("column").saveAsTable("Snappy_Table_Name")
```

In above example, Spark is of type SparkSession and SnappyData is of type SnappySession.
