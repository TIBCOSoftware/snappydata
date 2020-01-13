# Supported Data Formats

TIBCO ComputeDB relies on the Spark SQL Data Sources API to parallelly load data from a wide variety of sources. By integrating the loading mechanism with the Query engine (Catalyst optimizer) it is often possible to push down filters and projections all the way to the data source minimizing data transfer. Here is the list of important features:

**Support for many Sources** </br>There is built-in support for many data sources as well as data formats. Data can be accessed from S3, file system, HDFS, Hive, RDB, etc. And the loaders have built-in support to handle CSV, Parquet, ORC, Avro, JSON, Java/Scala Objects, etc as the data formats. 

**Access virtually any modern data store**</br> Virtually all major data providers have a native Spark connector that complies with the Data Sources API. For e.g. you can load data from any RDB like Amazon Redshift, Cassandra, Redis, Elastic Search, Neo4J, etc. While these connectors are not built-in, you can easily deploy these connectors as dependencies into a TIBCO ComputeDB cluster. All the connectors are typically registered in spark-packages.org

**Avoid Schema wrangling** </br>Spark supports schema inference. Which means, all you need to do is point to the external source in your 'create table' DDL (or Spark SQL API) and schema definition is learned by reading in the data. There is no need to explicitly define each column and type. This is extremely useful when dealing with disparate, complex and wide data sets. 

**Read nested, sparse data sets**</br> When data is accessed from a source, the schema inference occurs by not just reading a header but often by reading the entire data set. For instance, when reading JSON files the structure could change from document to document. The inference engine builds up the schema as it reads each record and keeps unioning them to create a unified schema. This approach allows developers to become very productive with disparate data sets.

**Load using Spark API or SQL** </br> You can use SQL to point to any data source or use the native Spark Scala/Java API to load. 
For instance, you can first [create an external table](../reference/sql_reference/create-external-table.md). 

```pre
CREATE EXTERNAL TABLE <tablename> USING <any-data-source-supported> OPTIONS <options>
```

Next, use it in any SQL query or DDL. For example,

```pre
CREATE EXTERNAL TABLE STAGING_CUSTOMER USING parquet OPTIONS(path 'quickstart/src/main/resources/customerparquet')

CREATE TABLE CUSTOMER USING column OPTIONS(buckets '8') AS ( SELECT * FROM STAGING_CUSTOMER)
```


The following data formats are supported in TIBCO ComputeDB:

*	CSV

*	Parquet

*	ORC

*	AVRO

*	JSON

*	Multiline JSON

*	XML

*	TEXT

The following table provides information about the supported data formats along with the methods to create an external table using SQL as well as API:

| **Format** |  **SQL** | **API** |
| --- | --- |--- |
| **CSV** | create external table staging_csv using csv options (path `'<csv_file_path>'`, delimiter ',', header 'true');</br>create table csv_test using column as select * from staging_csv;  |val extCSVDF =  snappy.createExternalTable("csvTable_ext","csv", Map("path"-> "`<csv_file_path>`" ,"header" -> "true", "inferSchema"->"true"),false)</br>snappy.createTable("csvTable", "column", extCSVDF.schema, Map("buckets"->"9"), false);</br>import org.apache.spark.sql.SaveMode;</br>extCSVDF.write.format("column").mode(SaveMode.Append).saveAsTable("csvTable")  |
| **Parquet** |create external table staging_parquet using parquet options (path '`<parquet_path>`');</br>create table parquet_test using column as select * from staging_parquet;|val extParquetDF = snappy.createExternalTable("parquetTable_ext","Parquet", Map("path"->"`<parquet_file_path>`"),false)</br>snappy.createTable("parquetTable", "column",extParquetDF.schema, Map("buckets"->"9"), false);</br>import org.apache.spark.sql.SaveMode;</br>extParquetDF.write.format("column").mode(SaveMode.Append).saveAsTable("parquetTable");  |
| **ORC** |create external table staging_orc using orc options (path `'<orc_path>'`);</br>create table orc_test using column as select * from staging_orc;  |   |
| **AVRO** | create external table staging_avro using com.databricks.spark.avro options (path `'<avro_file_path>'`);</br>create table avro_test using column as select * from staging_avro; | val extAvroDF = snappy.createExternalTable("avroTable_ext","com.databricks.spark.avro", Map("path"->"`<avro_file_path>`"),false)</br>snappy.createTable("avroTable", "column", extAvroDF.schema, Map("buckets"->"9"), false);</br>import org.apache.spark.sql.SaveMode;</br>extAvroDF.write.format("column").mode(SaveMode.Append).saveAsTable("avroTable");|
| **JSON** | create external table staging_json using json options (path `'<json_file_path>'`);</br>create table json_test using column as select * from staging_json;  |val extJsonDF = snappy.createExternalTable("jsonTable_ext","json", Map("path"-> "`<json_file_path>`"),false)</br>snappy.createTable("jsonTable", "column", extJsonDF.schema, Map("buckets"->"9"), false);</br>import org.apache.spark.sql.SaveMode;</br>extJsonDF.write.format("column").mode(SaveMode.Append).saveAsTable("jsonTable");   |
| **Multiline JSON** | create external table staging_json_multiline using json options (path '`<json_file_path>`', wholeFile 'true');</br>create table json_test using column as select * from staging_json_multiline;  |val extJsonMultiLineDF = snappy.createExternalTable("jsonTableMultiLine_ext","json", Map("path"-> "`<json_file_path>`","wholeFile" -> "true"),false)</br>snappy.createTable("jsonTableMultiLine", "column", extJsonMultiLineDF.schema, Map("buckets"->"9"), false);</br>import org.apache.spark.sql.SaveMode;</br>extJsonMultiLineDF.write.format("column").mode(SaveMode.Append).saveAsTable("jsonTableMultiLine");   |
| **XML** |create external table staging_xml using xml options (rowTag '`<rowTag>`',path '`<xml-file-path>`');</br>create table xml_test using column as select * from staging_xml;| val extXmlDF = snappy.createExternalTable("xmlTable_ext","xml", Map("path"-> "`<xml-file-path>`","rowTag" -> "row"),false)</br>snappy.createTable("xmlTable", "column", extXmlDF.schema, Map("buckets"->"9"), false)</br>;import org.apache.spark.sql.SaveMode;</br>extXmlDF.write.format("column").mode(SaveMode.Append).saveAsTable("xmlTable");|
| **TEXT** | create external table staging_text using text options (path '`<text_file_path>`');</br>create table text_test using column as select * from staging_text;  |   |