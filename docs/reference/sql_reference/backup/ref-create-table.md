#Create Table

```
CREATE [TEMPORARY] TABLE [IF NOT EXISTS] [db_name.]table_name
    [(col_name1[:] col_type1 [COMMENT col_comment1], ...)]
    USING datasource
    [OPTIONS (key1=val1, key2=val2, ...)]
    [PARTITIONED BY (col_name1, col_name2, ...)]
    [CLUSTERED BY (col_name3, col_name4, ...) INTO num_buckets BUCKETS]
    [AS select_statement]
```    
    
Create a table using a data source. If a table with the same name already exists in the database, an exception will be thrown.

**TEMPORARY**
The created table will be available only in this session and will not be persisted to the underlying metastore, if any. This may not be specified with IF NOT EXISTS or AS <select_statement>. To use AS <select_statement> with TEMPORARY, one option is to create a TEMPORARY VIEW instead.

```
CREATE TEMPORARY VIEW table_name AS select_statement
```

There is also “CREATE OR REPLACE TEMPORARY VIEW” that may be handy if you don’t care whether the temporary view already exists or not. Note that for TEMPORARY VIEW you cannot specify datasource, partition or clustering options since a view is not materialized like tables.

**IF NOT EXISTS**
If a table with the same name already exists in the database, nothing will happen. This may not be specified when creating a temporary table.

**USING `<data source>`**
Specify the file format to use for this table. The data source may be one of TEXT, CSV, JSON, JDBC, PARQUET, ORC, and LIBSVM, or a fully qualified class name of a custom implementation of org.apache.spark.sql.sources.DataSourceRegister.

**PARTITIONED BY**
The created table will be partitioned by the specified columns. A directory will be created for each partition.

**CLUSTERED BY**
Each partition in the created table will be split into a fixed number of buckets by the specified columns. This is typically used with partitioning to read and shuffle less data. Support for SORTED BY will be added in a future version.

**AS `<select_statement>`**
Populate the table with input data from the select statement. This may not be specified with TEMPORARY TABLE or with a column list. To specify it with TEMPORARY, use CREATE TEMPORARY VIEW instead.
Examples:

```
CREATE TABLE boxes (width INT, length INT, height INT) USING CSV

CREATE TEMPORARY TABLE boxes
    (width INT, length INT, height INT)
    USING PARQUET
    OPTIONS ('compression'='snappy')

CREATE TABLE rectangles
    USING PARQUET
    PARTITIONED BY (width)
    CLUSTERED BY (length) INTO 8 buckets
    AS SELECT * FROM boxes

CREATE OR REPLACE TEMPORARY VIEW temp_rectangles
    AS SELECT * FROM boxes
```

##Create Table with Hive format

```
CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [db_name.]table_name
    [(col_name1[:] col_type1 [COMMENT col_comment1], ...)]
    [COMMENT table_comment]
    [PARTITIONED BY (col_name2[:] col_type2 [COMMENT col_comment2], ...)]
    [ROW FORMAT row_format]
    [STORED AS file_format]
    [LOCATION path]
    [TBLPROPERTIES (key1=val1, key2=val2, ...)]
    [AS select_statement]

row_format:
    : SERDE serde_cls [WITH SERDEPROPERTIES (key1=val1, key2=val2, ...)]
    | DELIMITED [FIELDS TERMINATED BY char [ESCAPED BY char]]
        [COLLECTION ITEMS TERMINATED BY char]
        [MAP KEYS TERMINATED BY char]
        [LINES TERMINATED BY char]
        [NULL DEFINED AS char]

file_format:
    : TEXTFILE | SEQUENCEFILE | RCFILE | ORC | PARQUET | AVRO
    | INPUTFORMAT input_fmt OUTPUTFORMAT output_fmt
```

Create a table using the Hive format. If a table with the same name already exists in the database, an exception will be thrown. When the table is dropped later, its data will be deleted from the file system. Note: This command is supported only when Hive support is enabled.

**EXTERNAL**
The created table will use the custom directory specified with LOCATION. Queries on the table will be able to access any existing data previously stored in the directory. When an EXTERNAL table is dropped, its data is not deleted from the file system. This flag is implied if LOCATION is specified.

**IF NOT EXISTS**
If a table with the same name already exists in the database, nothing will happen.

**PARTITIONED BY**
The created table will be partitioned by the specified columns. This set of columns must be distinct from the set of non-partitioned columns. Partitioned columns may not be specified with AS <select_statement>.

**ROW FORMAT**
Use the SERDE clause to specify a custom SerDe for this table. Otherwise, use the DELIMITED clause to use the native SerDe and specify the delimiter, escape character, null character etc.

**STORED AS**
Specify the file format for this table. Available formats include TEXTFILE, SEQUENCEFILE, RCFILE, ORC, PARQUET and AVRO. Alternatively, the user may specify his own input and output formats through INPUTFORMAT and OUTPUTFORMAT. Note that only formats TEXTFILE, SEQUENCEFILE, and RCFILE may be used with ROW FORMAT SERDE, and only TEXTFILE may be used with ROW FORMAT DELIMITED.

**LOCATION**
The created table will use the specified directory to store its data. This clause automatically implies EXTERNAL.

** AS '<select_statement>' **
Populate the table with input data from the select statement. This may not be specified with PARTITIONED BY.

**Examples:**

```
CREATE TABLE my_table (name STRING, age INT)

CREATE EXTERNAL TABLE IF NOT EXISTS my_table (name STRING, age INT)
    COMMENT 'This table is created with existing data'
    LOCATION 'spark-warehouse/tables/my_existing_table'

CREATE TABLE my_table (name STRING, age INT)
    COMMENT 'This table is partitioned'
    PARTITIONED BY (hair_color STRING COMMENT 'This is a column comment')
    TBLPROPERTIES ('status'='staging', 'owner'='andrew')

CREATE TABLE my_table (name STRING, age INT)
    COMMENT 'This table specifies a custom SerDe'
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
    STORED AS
        INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'

CREATE TABLE my_table (name STRING, age INT)
    COMMENT 'This table uses the CSV format'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE

CREATE TABLE your_table
    COMMENT 'This table is created with existing data'
    AS SELECT * FROM my_table
```

Create Table Like

```
CREATE TABLE [IF NOT EXISTS] [db_name.]table_name1 LIKE [db_name.]table_name2
```

Create a table using the metadata of an existing table. The created table always uses its own directory in the default warehouse location even if the existing table is EXTERNAL. The existing table must not be a temporary table.