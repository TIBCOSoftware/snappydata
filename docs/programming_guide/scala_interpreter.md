# Executing Spark Scala Code using SQL  

!!!Note
	This is an experimental feature in the TIBCO ComputeDB 1.2.0 release.

Prior to the 1.2.0 release, any execution of a Spark scala program required the user to compile his Spark program, comply to specific callback API required by ComputeDB, package the classes into a JAR, and then submit the application using **snappy-job **tool. 
While, this procedure may still be the right option for a production application, it is quite cumbersome for the developer or data scientist wanting to quickly run some Spark code within the ComputeDB store cluster and iterate. 

With the introduction of the [**exec scala** SQL command](/reference/sql_reference/exec-scala.md), you can now get a JDBC or ODBC connection to the cluster and submit ad-hoc scala code for execution. The JDBC connection provides an ongoing session with the cluster so applications can maintain state and use/return this across multiple invocations.

Beyond this developer productivity appeal, this feature also allows you to skip using the [**Smart Connector**](/affinity_modes/connector_mode.md) in several cases.
Bespoke Spark applications using the smart connector are required to launch a client Spark application with its own executors and the store cluster is mostly providing parallel access to managed data. As the client is running queries this is additional capacity that you must budget. Moreover, the overall architecture is tough to understand. With **exec scala**, any Spark application could submit scala code as an SQL command now. 

**Here is one use case that motivated us**:  TIBCO data scientists using Team Studio (TIBCO Data Science platform) can now build custom operators, target TIBCO ComputeDB for in-database computations, and run adhoc Spark Scala code. For instance, ETL feature engineering using Spark ML or running a training job in-memory and with parallelism. 

Following are some examples to demonstrate the usage through some examples ....

### Example 1:
```
// Note this is a SQL command... this is the text you would send using a JDBC or ODBC connection

val prepDataCommand = 
  """ Exec scala 
         val dataDF = snapp.read.option("header", "true").csv("../path/to/customers.csv")
     // Variable 'snapp' is injected to your program automatically. Refers to a SnappySession instance.
     val newDF = dataDF.withColumn("promotion", "$20")
     newDF.createOrReplaceTempView("customers")
     //OR, store in in-memory table
     newDF.write.format("column").saveAsTable("customers")
"""
// Acquire JDBC connection and execute Spark program
Class.forName("io.snappydata.jdbc.ClientDriver")
val conn = DriverManager.getConnection("jdbc:snappydata://localhost:1527/")
conn.createStatement().executeSQL(prepDataCommand)
```

### Example 2:
```
 // Return back some Dataframe ... you use keyword returnDF to indicate the variable name in scala to return
 
val getDataFromCDB =
 """ exec scala options(returnDF 'someDF') 
       val someDF = snapp.table("customers").filter(..... )
 """
ResultSet rs = conn.createStatement().executeSQL(prepDataCommand)
//Use JDBC ResultSet API to fetch result. Data types will be mapped automatically
```

### Syntax

```
exec scala [options (returnDF ‘dfName’)] `<Scala_code>`k 
```

*	**exec** and **scala** are the keywords to identify this SQL type. 


*	**options** is an optional part of the syntax. If it is present, then after the keyword **options**, you can specify the allowed options inside parentheses. Currently, only one optional parameter, that is **returnDF**, can be specified with the execution. For this option, you can provide the name of any actual symbol in the Scala code, which is of type DataFrame. 
Through the **returnDF** option, you can request the system to return the result of the specific dataframe, which got created as the result of the Scala code execution. By default, the **exec scala** just returns the output of each interpreted line, which the interpreter prints on the Console after executing each line. 


## How does it work?

All the code from **exec scala** is executed using the Scala REPL <TODO: Link> on the TIBCO ComputeDB Lead node. When Spark Dataframes are invoked this would automatically result in workload distribution across all the TIBCO ComputeDB 
servers. The Lead node manages a pool of REPL based interpreters. The user SQL activity is delegated to one of the interpreters from this pool. The pool is lazily created.  

Any connection (JDBC or ODBC) results in the creation of a SnappySession within the CDB cluster. Moreover, the session remains associated with the connection until it is closed or dereferenced. 


The first time **exec scala** is executed, an interpreter from the pool gets associated with the connection. This allows the user to manage any adhoc private state on the server side. For example, any variables, objects, or even classes created will be isolated from other users.
The functioning of the interpreter is same as that of the interactive [Spark-shell](/programming_guide/using_the_spark_shell_and_spark-submit.md) only with one difference. As commands are interpreted any output generated will be cached in a buffer. And, when the command is done, the cached output will be available in the client side ResultSet object. 


<a id= secureexscala> </a>
### Securing the Usage of  exec scala SQL

The ability to run Scala code directly on a running cluster can be dangerous. This is because there are no checks on what code you can run. The submitted Scala code is executed on the lead node and has the potential to bring it down. It becomes essential to secure the use of this functionality.

By default, in a secure cluster, only the database owner is allowed to run Scala code through **exec scala** SQL or even through snappy-scala shell. The database owner is the user who brings up the TIBCO ComputeDB cluster. If different credentials are used for different components of the TIBCO ComputeDB cluster, then the credentials with which the lead node is started becomes the database owner for this purpose. Ideally, every node should start with the same superuser credentials.

The superuser or the database owner, in turn, can grant the privilege of executing Scala code to other users and even to entire LDAP groups. Similarly, it is only the superuser who can revoke this privilege from any user or LDAP group.

You can use the following DDL for this purpose:

**GRANT** 

```
grant privilege exec scala to <user(s) and or ldap_group(s)>
```

**Examples**

```
grant privilege exec scala to user1
revoke privilege exec scala from user1
grant privilege exec scala to user1,user2
grant privilege exec scala to LDAPGROUP:group1
revoke privilege exec scala from user2,LDAPGROUP:group1
```

### More Examples

The **snappy** CLI, which is also known as the snappy-sql CLI, is commonly used to fire SQL queries interactively into a TIBCO ComputeDB cluster. You can use these command-line tools to fire Scala code as well.

The following are some examples that show how to use the **exec scala **sql.

You can start a TIBCO ComputeDB cluster or connect to an already existing one using the snappy/snappy-sql CLI or you can obtain a JDBC/ODBC connection and fire these queries. Following examples were run on the snappy CLI.

**Example 1**

```
## A simple Scala code to define a value x and print it.

snappy> exec scala val x = 5
 /* This is a test
 program */
 // Just see the value of x
 println(x);
C0                                                                                                                              
--------------------------------------------------------------------------------------------------------------------------------
x: Int = 5
                                                                                                                                
5
```

**Example 2** 

```
## The following Scala code creates a table using the available snappy session. This can be accessed through the symbol ‘snappy’. It then inserts a couple of rows, obtains a dataframe object, and then uses the df.show command.

snappy> exec scala snappysession.sql("create table t1(c1 int not null)")
  /* This is a data frame test */
  // Check the collect output
  snappysession.sql("insert into t1 values (1), (2)")
 val df = snappysession.table("t1")
 val cnt = df.count
 df.show;
C0                                                                                                                              
--------------------------------------------------------------------------------------------------------------------------------
res4: org.apache.spark.sql.DataFrame = []                                                                                       
                                                                                                                                
                                                                                                                                
res7: org.apache.spark.sql.DataFrame = [count: int]                                                                             
df: org.apache.spark.sql.DataFrame = [c1: int]                                                                                  
cnt: Long = 2                                                                                                                   
+---+                                                                                                                           
| c1|                                                                                                                           
+---+                                                                                                                           
|  2|                                                                                                                           
|  1|                                                                                                                           
+---+  
```

!!!Note
	The variable **snappysession** is not declared anywhere; however, the above code uses it. The **snappysession** symbol name is for the object **SnappySession**, which represents this database connection. Similarly, **sc** is there to access the singleton SparkContext present on the lead node.

**Example 3** 

```
Declaring a case class and then creating a dataset using it. Also creating another dataset and then getting a dataframe on it.
Note the use of the option **returnDF** here. Through this option, you can request the system to return the result of the specific dataframe we want to which got created as the result of the Scala code execution. Here both **ds1** and **ds2** are created. However, the caller wants the output of the **ds2** and hence specified the symbol **ds2** in the options. By default, the exec scala returns the output of each interpreted line, which the interpreter prints on the Console after executing each line.

snappy> exec scala options(returnDF 'ds2') case class ClassData(a: String, b: Int)
      val sqlContext= new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._
      val ds1 = Seq(("a", 1), ("b", 2), ("c", 3)).toDF("a", "b").as[ClassData]
      var rdd = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)), 1)
      val ds2 = rdd.toDF("a", "b").as[ClassData];
a                                                                                                                               |b          
--------------------------------------------------------------------------------------------------------------------------------------------
a                                                                                                                               |1          
b                                                                                                                               |2          
c                                                                                                                               |3          

3 rows selected

```

!!!Note
	The use of the **sc** symbol here in this example. This is the global SparkContext present in the lead node.

### Known Issues and Limitations

An interpreter cannot serialize a dependent closure properly. A dependent closure is the closure that refers to some other class, function, or variable that is defined outside the closure.
Thus, the following example fails with closure serialization error.

```
exec scala def multiply(number: Int, factor: Int): Int = {

    number * factor

}

val data = Array(1, 2, 3, 4, 5)

val numbersRdd = sc.parallelize(data, 1)

val collectedNumbers = numbersRdd.map(multiply(_, 2)).collect()
```

The execution of the last line fails as the closure cannot be serialized due to this issue. This is referring to the function **multiply** that is defined outside the closure.

Similarly, even the following example fails:

```

val x = 5

val data = Array(1, 2, 3, 4, 5)

val numbersRdd = sc.parallelize(data, 1)

val collectedNumbers = numbersRdd.map(_ * x).collect()
```

This is because the closure is referring to **x**, which is defined outside the closure. 
There are no issues if the closure has no dependency on any external variables.