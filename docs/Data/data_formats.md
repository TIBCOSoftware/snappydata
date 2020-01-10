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
| **CSV** | create external table staging_csv using csv options (path `'<csv_file_path>'`, delimiter ',', header 'true');</br>create table csv_test using column as select * from staging_csv;  |   |
| **Parquet** |create external table staging_parquet using parquet options (path '`<parquet_path>`');</br>create table parquet_test using column as select * from staging_parquet;  |   |
| **ORC** |create external table staging_orc using orc options (path `'<orc_path>'`);</br>create table orc_test using column as select * from staging_orc;  |   |
| **AVRO** | create external table staging_avro using com.databricks.spark.avro options (path `'<avro_file_path>'`);</br>create table avro_test using column as select * from staging_avro; | val snappy = new org.apache.spark.sql.SnappySession(spark.sparkContext)</br>val avroData = snappy.read.format("com.databricks.spark.avro").load("`<avro-file-path>`")</br>avroData.createOrReplaceTempView("tempAvroTbl")</br>snappy.sql("CREATE TABLE avro_test using column AS SELECT * FROM tempAvroTbl") |
| **JSON** | create external table staging_json using json options (path `'<json_file_path>'`);</br>create table json_test using column as select * from staging_json;  |   |
| **Multiline JSON** | create external table staging_json_multiline using json options (path '`<json_file_path>`', wholeFile 'true');</br>create table json_test using column as select * from staging_json_multiline;  |   |
| **XML** |create external table staging_xml using xml options (rowTag '`<rowTag>`',path '`<xml-file-path>`');</br>create table xml_test using column as select * from staging_xml;| val snappy = new</br>org.apache.spark.sql.SnappySession(spark.sparkContext)</br>val xmlData = snappy.read.format("xml").option("rowTag","`<rowTag>`").load("`<xml-file-path>`")</br>xmlData.createOrReplaceTempView("tempXMLTbl")</br>snappy.sql("CREATE TABLE xml_test using column AS SELECT * FROM tempXMLTbl")|
| **TEXT** | create external table staging_text using text options (path '`<text_file_path>`');</br>create table text_test using column as select * from staging_text;  |   |