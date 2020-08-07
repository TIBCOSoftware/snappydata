<a id="getting-started-using-sql"></a> 
# Using SQL

In this section, you can also connect to SQL using Snappy Session API. </br>
You can use any SQL client tool (for example, Snappy Shell). For an example, refer to the [How-to](../howto/use_snappy_shell.md) section.

**Create a column table with a simple schema [Int, String] and default options**

For more information on the available options, refer to the [Row and Column Tables](../programming_guide/tables_in_snappydata.md) section.

```pre
scala>  snappy.sql("create table colTable(CustKey Integer, CustName String) using column options()")
```

```pre
//Insert couple of records to the column table
scala>  snappy.sql("insert into colTable values(1, 'a')")
scala>  snappy.sql("insert into colTable values(2, 'b')")
scala>  snappy.sql("insert into colTable values(3, '3')")
```

```pre
// Check the total row count now
scala>  snappy.sql("select count(*) from colTable").show
```

**Create a row table with a primary key**:

```pre
// Row formatted tables are better when data sets constantly change or access is selective (like based on a key).
scala>  snappy.sql("create table rowTable(CustKey Integer NOT NULL PRIMARY KEY, " +
            "CustName String) using row options()")
```
If you create a table using standard SQL (that is, no 'row options' clause) it creates a replicated row table.
 
```pre
//Insert couple of records to the row table
scala>  snappy.sql("insert into rowTable values(1, 'a')")
scala>  snappy.sql("insert into rowTable values(2, 'b')")
scala>  snappy.sql("insert into rowTable values(3, '3')")
```

```pre
//Update some rows
scala>  snappy.sql("update rowTable set CustName='d' where custkey = 1")
scala>  snappy.sql("select * from rowTable order by custkey").show
```

```pre
//Drop the existing tables
scala>  snappy.sql("drop table if exists rowTable ")
scala>  snappy.sql("drop table if exists colTable ")
```

```pre
scala> :q //Quit the Spark Shell
```

Now that you have seen the basic working of TIBCO ComputeDB tables, let us run the [benchmark](../quickstart/performance_apache_spark.md) code to see the performance of TIBCO ComputeDB and compare it to Spark's native cache performance.

## More Information

For more examples of the common operations, you can refer to the [How-tos](../howto.md) section. 

If you have questions or queries you can contact us through our [community channels](../techsupport.md#community).
