<a id="getting-started-using-sql"></a> 
##Option 4: Getting Started using SQL

We illustrate SQL using Spark SQL-invoked using the Session API. You can also use any SQL client tool (for example, Snappy Shell). For an example, refer to the [How-to](../how_to/use_sql_shell.md#howto-snappyShell) section.

**Create a column table with a simple schema [Int, String] and default options.**
For details on the options refer to the [Row and Column Tables](../pgm_guide/tables_in_snappydata.md#tables-in-snappydata) section.
```scala
scala>  snappy.sql("create table colTable(CustKey Integer, CustName String) using column options()")
```

```
//Insert couple of records to the column table
scala>  snappy.sql("insert into colTable values(1, 'a')")
scala>  snappy.sql("insert into colTable values(2, 'b')")
scala>  snappy.sql("insert into colTable values(3, '3')")
```

```scala
// Check the total row count now
scala>  snappy.sql("select count(*) from colTable").show
```

**Create a row table with primary key**:

```scala
//Row formatted tables are better when datasets constantly change or access is selective (like based on a key).
scala>  snappy.sql("create table rowTable(CustKey Integer NOT NULL PRIMARY KEY, " +
            "CustName String) using row options()")
```
If you create a table using standard SQL (i.e. no 'row options' clause) it creates a replicated Row table.
 
```scala
//Insert couple of records to the row table
scala>  snappy.sql("insert into rowTable values(1, 'a')")
scala>  snappy.sql("insert into rowTable values(2, 'b')")
scala>  snappy.sql("insert into rowTable values(3, '3')")
```

```scala
//Update some rows
scala>  snappy.sql("update rowTable set CustName='d' where custkey = 1")
scala>  snappy.sql("select * from rowTable order by custkey").show
```


```scala
//Drop the existing tables
scala>  snappy.sql("drop table if exists rowTable ")
scala>  snappy.sql("drop table if exists colTable ")
```

```
scala> :q //Quit the Spark Shell
```

Now that we have seen the basic working of SnappyData tables, let us run the [benchmark](#Start Benchmark) code to see the performance of SnappyData and compare it to Spark's native cache performance.