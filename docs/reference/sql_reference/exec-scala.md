# exec scala

## Syntax

		exec scala [options (returnDF ‘dfName’)] <Scala_code>

*	**exec** and **scala** are the keywords to identify this SQL type. 

*	**options** is an optional part of the syntax. If it is present, then after the keyword **options**, you can specify the allowed options inside parentheses. Currently, only one optional parameter, that is **returnDF**, can be specified with the execution. For this option, you can provide the name of any actual symbol in the Scala code, which is of type DataFrame. 
Through the **returnDF** option, you can request the system to return the result of the specific dataframe, which got created as the result of the Scala code execution. By default, the **exec scala** just returns the output of each interpreted line, which the interpreter prints on the Console after executing each line. 

## Description

**exec scala** is an SQL feature that you can use to submit Scala code to the SnappyData cluster. This is a SQL construct from any JDBC/ODBC client and one of the ways in which you can submit the Scala code to the SnappyData cluster. You can submit a chunk of Scala code similar to a SQL query, which is submitted to a database on a JDBC/ODBC connection including hive thrift server SQL clients such as beeline.

A parallel between a SQL query and a block of Scala code is brought about by using a fixed schema for the Scala code. Since, a select query's result set metadata is fixed, whereas there is no meaning of a fixed schema for a chunk of Scala code. Therefore, SnappyData provides a fixed schema to all the Scala code. This is elaborated in the following sections. 

### Important Information About **exec scala**

- Arbitrary Scala code can be executed through exec scala.
- A SnappySession object and the SparkContext object can be accessed through the Symbol name **snapp** and **sc** in the **exec scala** code.
- If you are having a loop inside your Scala code, then the output of each loop is not returned after the execution of each loop. The entire execution happens first, and after that, the output is fetched at once. This is unlike the spark-shell. In a spark-shell or Scala shell, the output of each loop execution is printed on the console.
- If you run rogue code such as **System.exit** or a divide by 0 scenario, the lead node is bound to crash. The system does not check the code before executing it.
- In a non-secured system, anyone can run **exec scala**.
- In a secured system only the dbowner or database owner can run **exec scala** by default.
- However, a database owner can grant and revoke privileges to and from users and LDAP groups.
- Multiple sessions can run **exec scala**. These sessions can be concurrent too. All the sessions are isolated from each other, that is any variables or/and types defined in one session that is visible only in the same session.
- By default, the output of this SQL is a single column resultset of type **varchar**. Essentially, whatever the interpreter writes on the console is brought back to the caller/user.
- The user can specify the name of the dataframe instead as a return result instead of the default behavior. The symbol should be of a valid existing dataframe symbol in the context of the session.

## Example

A simple Scala code to define a value x and print it.

```
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




