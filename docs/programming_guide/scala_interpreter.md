# Using Scala Interpreter 

Users familiar with the command-line interpreter tools like Scala can experiment, play around with Scala programming language easily, quickly, and if needed, in frequent iterations. All you need to do is install the Scala interpreter like any other utility. You do not have to compile the code first and then run it. Usually, any small change required would not only mean modifying the source code but also recompile, re-deploy, or re-run it. Such interpreters are even more convenient if you are doing something which is highly iterative. 
Apache Spark has a command-line utility called **spark-shell**, which is primarily built on the Scala interpreter. Through this utility, you can launch spark-shell with any of the cluster managers. It can be launched in a local mode as well as in a client mode. 
<Link to Spark documentation>

If you want to use a spark-shell to connect to a TIBCO ComputeDB cluster, you can do so only in the Smart Connector mode. <Link to Smart Connector mode in documentation> However, if you want to execute any code in the store cluster, then you have to comply with the contract that is defined per Spark Job Server, compile, and package the code in a JAR and then deploy the code using the Snappy-job command. This is cumbersome, especially for developers who want to iterate over the code quickly. Moreover, debugging such jobs is tricky as you must execute the job asynchronously, and any error requires you to track the JobIDs and poll the system to know the fate of the job.

TIBCO ComputeDB provides the following, on an experimental basis, to facilitate the execution of scala code directly in an Embedded cluster mode. 

*	**exec scala SQL**</br>
	This is a SQL construct from any JDBC/ODBC client and one of the ways in which you can submit the Scala code to the TIBCO ComputeDB cluster.

* 	**Scala interpreter CLI utility**</br>
	You can also submit Scala code to the TIBCO ComputeDB cluster using this command-line utility, which is similar to a spark-shell.


## exec scala

**exec scala** is a SQL capability that you can use to submit scala code to the TIBCO ComputeDB cluster. You can submit a chunk of Scala code similar to a SQL query, which is submitted to a database on a JDBC/ODBC connection.

A parallel between a SQL query and a block of scala code is brought about by using a fixed schema for the scala code. Since, a select query's result set metadata is fixed where as there is no meaning of a fixed schema for a chunk of scala code. Therefore, TIBCO ComputeDB provides a fixed schema to all the scala code. This is elaborated in the following sections.

### Syntax

```
exec scala [options (returnDF ‘dfName’)] <scala_code> 
```

*	**exec** and **scala** are the keywords to identify this SQL type. 

*	**options** is an optional part of the syntax. If it is present, then after the keyword **options**, you can specify the allowed options inside parentheses. Currently, only one optional parameter, that is **returnDF**, can be specified with the execution. For this option, you can provide the name of any actual symbol in the scala code, which is of type DataFrame. 
Through the **returnDF** option, you can request the system to return the result of the specific dataframe, which got created as the result of the scala code execution. By default, the **exec scala** just returns the output of each interpreted line, which the interpreter prints on the Console after executing each line. 

For more details, refer to the Examples section.

<a id= secureexscala> </a>
### Securing the Usage of  exec scala SQL

The ability to run a scala code directly into a running cluster can affect the health of the cluster. This is because there is no check on the type of code that you want to execute in the cluster. The submitted scala code is taken to the lead node and is executed there. An erroneous code can bring down the lead node and thereby make the TIBCO ComputeDB cluster unavailable. Hence, it becomes essential to secure the use of this functionality.

By default, in a secure cluster, only the database owner is allowed to run scala code through **exec scala** SQL or even through snappy-scala. The database owner is the user who brings up the TIBCO ComputeDB cluster. If different credentials are used for different components of the TIBCO ComputeDB cluster, then the credentials with which the lead node is started becomes the database owner for this purpose. Ideally, every node should start with the same superuser credentials.

The superuser or the database owner, in turn, can grant the privilege of executing scala code to other users and even to entire LDAP groups. Similarly, it is only the superuser who can revoke this privilege from any user or LDAP group.

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

### Examples

The **snappy** CLI, which is also known as the snappy-SQL CLI, is commonly used to fire SQL queries interactively into a TIBCO ComputeDB cluster. You can use these command-line tools to fire scala code as well.

The following are some examples that show how to use the **exec scala **sql.

You can start a TIBCO ComputeDB cluster or connect to an already existing one using the snappy/snappy-sql CLI or you can obtain a jdbc/odbc connection and fire these queries. Following examples were run on the snappy CLI.

**Example 1**

```
## A simple scala code to define a value x and print it.

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
## The following scala code creates a table using the available snappy session. This can be accessed through the symbol ‘snappy’. It then inserts a couple of rows, obtains a dataframe object, and then uses the df.show command.

snappy> exec scala snappy.sql("create table t1(c1 int not null)")
  /* This is a data frame test */
  // Check the collect output
  snappy.sql("insert into t1 values (1), (2)")
 val df = snappy.table("t1")
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
	The variable **session** is not declared anywhere; however, the above code uses it. This **snappy** symbol name is for the object **SnappySession**, which represents this database connection. Similarly, **sc** is there to access the singleton SparkContext present on the lead node.

**Example 3** 

```
Declaring a case class and then creating a dataset using it. Also creating another dataset and then getting a dataframe on it.
Note the use of the option **returnDF** here. Through this option, you can request the system to return the result of the specific dataframe we want to which got created as the result of the scala code execution. Here both **ds1** and **ds2** are created. However, the caller wants the output of the **ds2** and hence specified the symbol **ds2** in the options. By default, the exec scala returns the output of each interpreted line, which the interpreter prints on the Console after executing each line.

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

### Important Information About **exec scala**

- Arbitrary scala code can be executed through exec scala.
- A SnappySession object and the SparkContext object can be accessed through the Symbol name **snapp** and **sc** in the **exec scala** code.
- If you are having a loop inside your scala code, then the output of each loop is not returned after the execution of each loop. The entire execution happens first, and after that, the output is fetched at once. This is unlike the spark-shell. In a spark-shell or scala shell, the output of each loop execution is printed on the console.
- If you run rogue code such as **System.exit** or a divide by 0 scenario, the lead node is bound to crash. The system does not check the code before executing it.
- In a non-secured system, anyone can run **exec scala**.
- In a secured system only the dbowner or database owner can run **exec scala** by default.
- However, a database owner can grant and revoke privileges to and from users and LDAP groups.
- Multiple sessions can run **exec scala**. These sessions can be concurrent too. All the sessions are isolated from each other, that is any variables or/and types defined in one session that is visible only in the same session.
- By default, the output of this SQL is a single column resultset of type **varchar**. Essentially, whatever the interpreter writes on the console is brought back to the caller to the user.
- The user can specify the name of the dataframe instead as a return result instead of the default behavior. The symbol should be of a valid existing dataframe symbol in the context of the session.

### Known Issues and Limitations

An interpreter cannot serialize a dependent closure properly. A dependent closure is the closure that refers to some other class, function, or variabl that is defined outside the closure.
Thus, the following example fails with closure serialization error.

```
exec scala def multiply(number: Int, factor: Int): Int = {

    number \* factor

}

val data = Array(1, 2, 3, 4, 5)

val numbersRdd = sc.parallelize(data, 1)

val collectedNumbers = numbersRdd.map(multiply(\_, 2).toString()).collect()
```

The execution of the last line fails as the closure cannot be serialized due to this issue. This is referring to the function **multiple** that is defined outside the closure.

Similarly, even the following example fails:

```

Val x = 5

val data = Array(1, 2, 3, 4, 5)

val numbersRdd = sc.parallelize(data, 1)

val collectedNumbers = numbersRdd.map(\_ \* x).toString()).collect()
```

This is because the closure is referring to **x**, which is defined outside the closure. 
There are no issues if the closure has no dependency on any external variables.

## snappy-scala CLI

The snappy-scala CLI, which is similar to spark-shell in its capabilities, is introduced as an experimental feature in this release. snappy-scala CLI is built on top of the **exec scala** feature and the already existing snappy CLI utility. This is not a true scala interpreter but mimics a scala or a spark-shell kind of interpreter. Here each line of code is shipped to the lead node. The code is interpreted on the lead node. The result is brought back to the snappy-scala shell.

Features such as auto-complete and full-fledged colon command capabilities are not as stable as in the spark-shell. This is mainly because in Apache Spark, the spark-shell is itself the Application driver VM. Whereas, in TIBCO ComputeDB, the driver is the lead node, which is a remote process from the command-line utility point of view. The utility connects like a client utility and therefore has the limitation mentioned above. Those limitations can be overcome and may be considered in a future release.

### Command-line options

```
$ ./bin/snappy-scala -h

Usage:

snappy-scala [OPTIONS]

OPTIONS and Default values

   -c LOCATOR_OR_SERVER_ENDPOINT  (default value is locahost:1527)

   -u USERNAME                    (default value is APP)

   -p PASSWORD                    (default value is APP)

   -r SCALA_FILE_PATHS            (comma separated paths if multiple)

   -h, --help                     (prints script usage)

```


As seen above there are 5

### Securing the Usage of snappy-scala CLI

This is the same as securing the usage of **exec scala**. For more details refer to [Securing the Usage of  exec scala SQL](#secureexscala) 