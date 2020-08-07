# allow-explicit-commit

## Description
Using this property, you can specify whether to allow the execution of unsupported operations. Such operations may otherwise produce an error when you have set the JDBC autocommit to false using **java.sql.Connection#setAutoCommit** API. 
If you set the autocommit to false, the operations in a column table produces an error as follows: 
**Operations on column tables are not supported when query routing is disabled or autocommit is false**. 
To allow such operations, set the **allow-explicit-commit** property to true.

!!!Note 
	Although this property allows using the JDBC autocommit(false) and commit/rollback APIs, all of these are no-op with no change in the product behavior. This means that autocommit is always true even if the user sets it explicitly to false.

This property is useful in scenarios as SQL client tools that may use transactions isolation levels (read committed / repeatable read) and explicitly set autocommit to false. In such cases, without this property, the SQL operations produce an error.

## Example 
This property can be used in connection URLs while connecting to SnappyData JDBC server. In such a case JDBC URL appears as follows:

**jdbc:snappydata://locatoHostName:1527/allow-explicit-commit=true**

This property can also be passed to **java.sql.DriverManager#getConnection(java.lang.String, java.util.Properties)** API in the properties object.

## Default Value
false

## Property Type
Connection

## Prefix
NA
