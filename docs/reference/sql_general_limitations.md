# Limitations

The following SQL general limitations are observed in SnappyData:

*	[For row tables without primary key, DML operations that use Spark functions are not supported](#limitation1)
*	[The syntax, `INSERT INTO <table><(col1,...)> values (… )`, cannot contain Spark functions in the values clause](#limitation2).
*	[For complex data types (ARRAY, MAP, STRUCT), values to be inserted can not be directly used in the values clause of `INSERT INTO <table> values (x, y, z … )`](#limitation3)

<a id="limitation1"></a>
##### For row tables without primary key, DML operations that use Spark functions are not supported

!!!Note
	This limitation applies only to row tables. For column tables such DML operations are supported.

In the current release of SnappyData, row tables must contain a primary key column, if a DML operation on the table uses Spark function. For example, functions such as **spark_partition_id()**, **current_timestamp()**.

In the following example, *table1* is a row table without primary key. As shown, in such a case **UPDATE** operations that use Spark functions produce an error:


        snappy> create table table1(c1 int, c2 timestamp, c3 string) using row options (partition_by 'c1');
        snappy> insert into table1 values(1, '2019-07-22 14:29:22.432', 'value1');
        1 row inserted/updated/deleted
        snappy> insert into table1 values(2, '2019-07-22 14:29:22.432', 'value2');
        1 row inserted/updated/deleted

        snappy> update table1 set c3 = 'value3' where SPARK_PARTITION_ID() = 6;
        ERROR 42Y03: (SQLState=42Y03 Severity=20000) (Server=localhost/127.0.0.1[1528] Thread=ThriftProcessor-0) 'SPARK_PARTITION_ID)' is not recognized as a function or procedure.

        snappy> update table1 set c2 = current_timestamp() where c1 = 2;
        ERROR 42X01: (SQLState=42X01 Severity=20000) (Server=localhost/127.0.0.1[1528] Thread=ThriftProcessor-0) Syntax error: Encountered "(" at line 1, column 41.

However, if *table1* contains a primary key, then the DML operations are supported. In the following example, *table1* is now created with *column c1* as a primary key so the **UPDATE** operations succeed. 

        snappy> create table table1(c1 int primary key, c2 timestamp, c3 string) using row options (partition_by 'c1');
        snappy> insert into table1 values(1, '2019-07-22 14:29:22.432', 'value1');
        1 row inserted/updated/deleted
        snappy> insert into table1 values(2, '2019-07-22 14:29:22.432', 'value2');
        1 row inserted/updated/deleted
        snappy> update table1 set c3 = 'value3' where SPARK_PARTITION_ID() = 6;
        1 row inserted/updated/deleted
        snappy> update table1 set c2 = current_timestamp() where c1 = 2;
        1 row inserted/updated/deleted
        snappy> select * from table1;
        c1         |c2                        |c3             
        ------------------------------------------------------
        1          |2019-07-22 14:29:22.432   |value1         
        2          |2019-07-22 14:36:47.879   |value3         

        2 rows selected

<a id="limitation2"></a>
##### The syntax, `INSERT INTO <table><(col1,...)> values (… )`, cannot contain Spark functions in the values clause

The value clause of `INSERT INTO <table><(col1,...)> values (… ) `operation can not contain Spark functions. In such a case, use syntax `INSERT INTO <table> SELECT <>` syntax.

In the following example, insert operation fails as **current_timestamp()** function is used in the values:


        snappy> create table table1(c1 int, c2 timestamp, c3 string) using row options (partition_by 'c1');

        snappy> insert into table1(c1, c2, c3) values(1, current_timestamp(), 'value1');
        ERROR 38000: (SQLState=38000 Severity=20000) (Server=localhost/127.0.0.1[1528] Thread=ThriftProcessor-0) The exception 'com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException: myID: 127.0.0.1(835)<v1>:23712, caused by java.lang.AssertionError: assertion failed: No plan for DMLExternalTable INSERT INTO "TABLE1"("C1", "C2", "C3") values(1, current_timestamp(), 'value1')
        +- LocalRelation [col1#42, col2#43, col3#44]
        ' was thrown while evaluating an expression.


However, the following syntax works:

        snappy> insert into table1 select 1, current_timestamp(), 'value1';
        1 row inserted/updated/deleted
        snappy> select * from table1;
        c1         |c2                        |c3             
        ------------------------------------------------------
        1          |2019-07-22 14:49:20.022   |value1         

        1 row selected

<a id="limitation3"></a>
##### For complex data types (ARRAY, MAP, STRUCT), values to be inserted can not be directly used in the values clause of `INSERT INTO <table> values (x, y, z … )`

To insert values using Snappy shell or a SQL client use `insert into <table> select` syntax.

For example: 

        # create a table with column of type MAP and insert few records
        snappy> CREATE TABLE IF NOT EXISTS StudentGrades (rollno Integer, name String, Course Map<String, String>) USING column;
        snappy> INSERT INTO StudentGrades SELECT 1,'Jim', Map('English', 'A+');
        1 row inserted/updated/deleted
        # create a table with column of type ARRAY
        snappy> CREATE TABLE IF NOT EXISTS Student(rollno Int, name String, marks Array<Double>) USING column;
        snappy> INSERT INTO Student SELECT 1,'John', Array(97.8,85.2,63.9,45.2,75.2,96.5);
        1 row inserted/updated/deleted
        # create a table with column of type STRUCT
        snappy> CREATE TABLE IF NOT EXISTS StocksInfo (SYMBOL STRING, INFO STRUCT<TRADING_YEAR: STRING, AVG_DAILY_VOLUME: LONG, HIGHEST_PRICE_IN_YEAR: INT, LOWEST_PRICE_IN_YEAR: INT>) USING COLUMN;
        snappy> INSERT INTO StocksInfo SELECT 'ORD', STRUCT('2018', '400000', '112', '52');
        1 row inserted/updated/deleted


The following syntax will produce an error:


        snappy> insert into StudentGrades values (1, 'Jim',Map('English', 'A', 'Science', 'B'));
        ERROR 42Y03: (SQLState=42Y03 Severity=20000) (Server=localhost/127.0.0.1[1529] Thread=ThriftProcessor-0) 'MAP(java.lang.String,java.lang.String,java.lang.String,java.lang.String)' is not recognized as a function or procedure.

For more details on complex datatypes, refer to [Supported datatypes](misc/supported_datatypes.md) and [how to store and retrieve complex data types using ComplexTypeSerializer class](/howto/store_retrieve_complex_datatypes_JDBC.md).