# exec scala

## Description

**exec scala** is an SQL feature that you can use to submit Scala code to the TIBCO ComputeDB cluster. This is a SQL construct from any JDBC/ODBC client and one of the ways in which you can submit the Scala code to the TIBCO ComputeDB cluster. You can submit a chunk of Scala code similar to a SQL query, which is submitted to a database on a JDBC/ODBC connection including hive thrift server SQL clients such as beeline.

A parallel between a SQL query and a block of Scala code is brought about by using a fixed schema for the Scala code. Since, a select query's result set metadata is fixed, whereas there is no meaning of a fixed schema for a chunk of Scala code. Therefore, TIBCO ComputeDB provides a fixed schema to all the Scala code. This is elaborated in the following sections. 

### How Does it Work?

All the code from **exec scala** is executed using the Scala REPL <TODO: Link> on the TIBCO ComputeDB Lead node. When Spark Dataframes are invoked this would automatically result in workload distribution across all the TIBCO ComputeDB 
servers. The Lead node manages a pool of REPL based interpreters. The user SQL activity is delegated to one of the interpreters from this pool. The pool is lazily created.  

Any connection (JDBC or ODBC) results in the creation of a SnappySession within the CDB cluster. Moreover, the session remains associated with the connection until it is closed or dereferenced. 


The first time **exec scala** is executed, an interpreter from the pool gets associated with the connection. This allows the user to manage any adhoc private state on the server side. For example, any variables, objects, or even classes created will be isolated from other users.
The functioning of the interpreter is same as that of the interactive [Spark-shell](/programming_guide/using_the_spark_shell_and_spark-submit.md) only with one difference. As commands are interpreted any output generated will be cached in a buffer. And, when the command is done, the cached output will be available in the client side ResultSet object. 

### Important Information about **exec scala**

- Arbitrary Scala code can be executed through exec scala.
- A SnappySession object and the SparkContext object can be accessed through the Symbol name **snappysession** and **sc** in the **exec scala** code.
- If you are having a loop inside your Scala code, then the output of each loop is not returned after the execution of each loop. The entire execution happens first, and after that, the output is fetched at once. This is unlike the spark-shell. In a spark-shell or Scala shell, the output of each loop execution is printed on the console.
- If you run rogue code such as **System.exit** or a divide by 0 scenario, the lead node is bound to crash. The system does not check the code before executing it.
- In a non-secured system, anyone can run **exec scala**.
- In a secured system only the dbowner or database owner can run **exec scala** by default.
- However, a database owner can grant and revoke privileges to and from users and LDAP groups.
- Multiple sessions can run **exec scala**. These sessions can be concurrent too. All the sessions are isolated from each other, that is any variables or/and types defined in one session that is visible only in the same session.
- By default, the output of this SQL is a single column resultset of type **varchar**. Essentially, whatever the interpreter writes on the console is brought back to the caller/user.
- The user can specify the name of the dataframe instead as a return result instead of the default behavior. The symbol should be of a valid existing dataframe symbol in the context of the session.


## Syntax

		exec scala [options (returnDF ‘dfName’)] <Scala_code>
        
*	**exec** and **scala** are the keywords to identify this SQL type. 

*	**options** is an optional part of the syntax. If it is present, then after the keyword **options**, you can specify the allowed options inside parentheses. Currently, only one optional parameter, that is **returnDF**, can be specified with the execution. For this option, you can provide the name of any actual symbol in the Scala code, which is of type DataFrame. 
Through the **returnDF** option, you can request the system to return the result of the specific dataframe, which got created as the result of the Scala code execution. By default, the **exec scala** just returns the output of each interpreted line, which the interpreter prints on the Console after executing each line. 

<a id= examplesofexec> </a>
## Examples

### Examples I

Following are some examples to demonstrate the usage of **exec scala**. You can run these examples using [Snappy shell](../../howto/use_snappy_shell.md).


*	A simple Scala code to define a value **x** and print it.

            exec scala val x = 5
            /* This is a test
            program */
            // Just see the value of x
            println(x);
            C0                                                                                                                              
            ------------------------------------------------------------------------------------------------------
            x: Int = 5

            5


*	Create table using the available snappy session, which can be accessed through the variable ‘snappysession’. It then inserts a couple of rows, obtains a dataframe object, and then uses the **df.show** command.

			snappy> exec scala snappysession.sql("create table t1(c1 int not null)")
            /* This is a data frame test */
            // Check the collect output
            snappysession.sql("insert into t1 values (1), (2)")
            val df = snappysession.table("t1")
            val cnt = df.count
            df.show;
            C0                                                                                                                              
            --------------------------------------------------------------------------------------------------
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

    !!!Note
        The variable **snappysession** is not declared anywhere; however, the above code uses it. The **snappysession** symbol name is for the object **SnappySession**, which represents this database connection. Similarly, **sc** is there to access the singleton SparkContext present on the lead node.
    
### Examples II

*	Executing scala code snippet via JDBC or ODBC connection on to the TIBCO ComputeDB cluster.

            // Note this is an SQL command... this is the text you will send using a JDBC or ODBC connection.

            val prepDataCommand = 
              """ exec scala 
                 val dataDF = snappysession.read.option("header", "true").csv("../path/to/customers.csv")
                 // Variable 'snappysession' is injected to your program automatically. 
                 // Refers to a SnappySession instance.
                 val newDF = dataDF.withColumn("promotion", "$20")
                 newDF.createOrReplaceTempView("customers")
                 //OR, store in in-memory table
                 newDF.write.format("column").saveAsTable("customers")
              """
            // Acquire JDBC connection and execute Spark program
            Class.forName("io.snappydata.jdbc.ClientDriver")
            val conn = DriverManager.getConnection("jdbc:snappydata://localhost:1527/")
            conn.createStatement().executeSQL(prepDataCommand)

*	Returning a specific Dataframe using keyword **returnDF**. Note the use of the option **returnDF** here. Through this option, you can request the system to return the result of the specific dataframe you want, which got created as the result of the Scala code execution.

            val getDataFromCDB =
             """ exec scala options(returnDF 'someDF') 
                   val someDF = snappysession.table("customers").filter(..... )
             """
            ResultSet rs = conn.createStatement().executeSQL(getDataFromCDB)
            //Use JDBC ResultSet API to fetch result. Data types will be mapped automatically

*	Declaring a case class and then creating a dataset using it. Also creating another dataset and then getting a dataframe on it. Note the use of the option **returnDF** here. Through this option, you can request the system to return the result of the specific dataframe you want, which got created as the result of the Scala code execution. 
Here both **ds1** and **ds2** are created. However, the caller wants the output of the **ds2** and hence specified the symbol **ds2** in the options. By default, the **exec scala** returns the output of each interpreted line, which the interpreter prints on the console after executing each line.

            exec scala options(returnDF 'ds2') case class ClassData(a: String, b: Int)
            val sqlContext= new org.apache.spark.sql.SQLContext(sc)
            import sqlContext.implicits._
            val ds1 = Seq(("a", 1), ("b", 2), ("c", 3)).toDF("a", "b").as[ClassData]
            var rdd = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)), 1)
            val ds2 = rdd.toDF("a", "b").as[ClassData];
            a                                                          |b          
            ---------------------------------------------------------------------------
            a                                                          |1          
            b                                                          |2          
            c                                                          |3          

            3 rows selected


    !!!Note
        The use of the **sc** symbol here in this example. This is the global SparkContext present in the lead node.


<a id= secureexscala> </a>
### Securing the Usage of exec scala SQL

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
