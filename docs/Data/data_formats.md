# Supported Data Formats

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
| --- | --- | --- | --- | --- |
| **CSV** | create external table staging_csv using csv options (path `'<csv_file_path>'`, delimiter ',', header 'true');</br>create table csv_test using column as select * from staging_csv;  |   |
| **Parquet** |create external table staging_parquet using parquet options (path '`<parquet_path>`');</br>create table parquet_test using column as select * from staging_parquet;  |   |
| **ORC** |create external table staging_orc using orc options (path `'<orc_path>'`);</br>create table orc_test using column as select * from staging_orc;  |   |
| **AVRO** | create external table staging_avro using com.databricks.spark.avro options (path `'<avro_file_path>'`);</br>create table avro_test using column as select * from staging_avro; | val snappy = new org.apache.spark.sql.SnappySession(spark.sparkContext)</br>val avroData = snappy.read.format("com.databricks.spark.avro").load("`<avro-file-path>`")</br>avroData.createOrReplaceTempView("tempAvroTbl")</br>snappy.sql("CREATE TABLE avro_test using column AS SELECT * FROM tempAvroTbl") |
| **JSON** | create external table staging_json using json options (path `'<json_file_path>'`);</br>create table json_test using column as select * from staging_json;  |   |
| **Multiline JSON** | create external table staging_json_multiline using json options (path '`<json_file_path>`', wholeFile 'true');</br>create table json_test using column as select * from staging_json_multiline;  |   |
| **XML** |create external table staging_xml using xml options (rowTag '`<rowTag>`',path '`<xml-file-path>`');</br>create table xml_test using column as select * from staging_xml;| val snappy = new</br>org.apache.spark.sql.SnappySession(spark.sparkContext)</br>val xmlData = snappy.read.format("xml").option("rowTag","`<rowTag>`").load("`<xml-file-path>`")</br>xmlData.createOrReplaceTempView("tempXMLTbl")</br>snappy.sql("CREATE TABLE xml_test using column AS SELECT * FROM tempXMLTbl")|
| **TEXT** | create external table staging_text using text options (path '`<text_file_path>`');</br>create table text_test using column as select * from staging_text;  |   |