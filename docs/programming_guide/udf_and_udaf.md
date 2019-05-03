# User Defined Functions (UDF) and User Defined Aggregate Functions (UDAF)
Users can define a function and completely customize how TIBCO ComputeDB evaluates data and manipulates queries using UDF and UDAF functions across sessions. 
The definition of the functions is stored in a persistent catalog, which enables it to be used after node restart as well.

!!! Note
	Support for UDF is available from TIBCO ComputeDB version release 0.8 onwards.

## Create User Defined Function

You can simply extend any one of the interfaces in the package **org.apache.spark.sql.api.java**. 
These interfaces can be included in your client application by adding **snappy-spark-sql_2.11-2.0.3-2.jar** to your classpath.

### Define a User Defined Function class

The number of the interfaces (UDF1 to UDF22) signifies the number of parameters a UDF can take.

!!! Note
	Currently, any UDF which can take more than 22 parameters is not supported.

```pre
package some.package
import org.apache.spark.sql.api.java.UDF1

class StringLengthUDF extends UDF1[String, Int] {
 override def call(t1: String): Int = t1.length
}
```

<a id= create_udf> </a>
### Create a User Defined Function

!!! Note
	Place the jars used for creating persistent UDFs in a shared location (NFS, HDFS etc.) if you are configuring multiple leads for high availability. The same jar is used for DDL replay while the standby lead becomes the active lead.
    
After defining a UDF you can bundle the UDF class in a JAR file and create the function by using `./bin/snappy-sql` of TIBCO ComputeDB. This creates a persistent entry in the catalog after which, you use the UDF.

```pre
CREATE FUNCTION udf_name AS qualified_class_name RETURNS data_type USING JAR '/path/to/file/udf.jar'
```

For example:

```pre
CREATE FUNCTION APP.strnglen AS some.package.StringLengthUDF RETURNS Integer USING JAR '/path/to/file/udf.jar'
```

You can write a JAVA or SCALA class to write a UDF implementation. 

!!! Note 
	For input/output types: </br>
	The framework always returns the Java types to the UDFs. So, if you are writing `scala.math.BigDecimal` as an input type or output type, an exception is reported. You can use `java.math.BigDecimal` in the SCALA code. 

**Return Types to UDF program type mapping**

| TIBCO ComputeDB Type | UDF Type |
| --- | --- |
|STRING|java.lang.String|
|INTEGER|java.lang.Integer|
|LONG|java.lang.Long|
|DOUBLE|java.lang.Double|
|DECIMAL|java.math.BigDecimal|
|DATE|java.sql.Date|
|TIMESTAMP|java.sql.Timestamp|
|FLOAT|java.lang.Float|
|BOOLEAN|java.lang.Boolean|
|SHORT|java.lang.Short|
|BYTE|java.lang.Byte|

## Use a User Defined Function

```pre
select strnglen(string_column) from <table>
```

If you try to use a UDF on a different type of column, for example, an **Int** column an exception is reported.


#### Drop the Function

```pre
DROP FUNCTION IF EXISTS udf_name
```

For example:

```pre
DROP FUNCTION IF EXISTS app.strnglen
```

#### Modify an Existing User Defined Function

 1) Drop the existing UDF

 2) Modify the UDF code and [create a new UDF](#create_udf). You can create the UDF with the same name as that of the dropped UDF.


## Create User Defined Aggregate Functions

TIBCO ComputeDB uses the same interface as that of Spark to define a User Defined Aggregate Function  `org.apache.spark.sql.expressions.UserDefinedAggregateFunction`. For more information refer to this [document](https://databricks.com/blog/2015/09/16/apache-spark-1-5-dataframe-api-highlights.html).

## Known Limitations
*	In the current version of the product, setting schema over a JDBC connection (using the `set schema` command) or Snappy Session (using `SnappySession.setSchema` API) does not work in all scenarios. Even if the schema is set, the operations are occasionally performed in the default `APP` schema. 
As a workaround, you can qualify the schemaname with tablename. </br> 
For example, to select all rows from table 't1' in schema 'schema1', use query `select * from schema1.t1`

*	In the current version of the product, user defined functions are not displayed when you run the SHOW FUNCTIONS command in Snappy Shell. This will be available in the future releases.
