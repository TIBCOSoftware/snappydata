# CREATE FUNCTION

```pre
CREATE FUNCTION udf_name AS qualified_class_name RETURNS data_type USING JAR '/path/to/file/udf.jar'
```

## Description

Creates a function. Users can define a function and completely customize how SnappyData evaluates data and manipulates queries using UDF and UDAF functions across sessions. The definition of the functions is stored in a persistent catalog, which enables it to be used after node restart as well.

You can extend any one of the interfaces in the package `org.apache.spark.sql.api.java`. These interfaces can be included in your client application by adding `snappy-spark-sql_2.11-2.0.3-2.jar` to your classpath.


!!! Note
	For input/output types: </br> The framework always returns the Java types to the UDFs. So, if you are writing `scala.math.BigDecimal` as an input type or output type, an exception is reported. You can use `java.math.BigDecimal` in the SCALA code.


**Return Types to UDF Program Type Mapping**

| SnappyData Type | UDF Type |
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
|CHAR| java.lang.String|
|VARCHAR|java.lang.String|

## Example
```pre
CREATE FUNCTION APP.strnglen AS some.package.StringLengthUDF RETURNS Integer USING JAR '/path/to/file/udf.jar'
```

You can write a JAVA or SCALA class to write an UDF implementation. 

**Related Topics**

* [DROP FUNCTION](drop-function.md)
