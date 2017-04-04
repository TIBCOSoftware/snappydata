### Create User Defined Function

You can simply extend any one of the interfaces in the package **org.apache.spark.sql.api.java**. 
These interfaces can be included in your client application by adding **snappy-spark-sql_2.11-2.0.3-2.jar** to your classpath.

#### **Define a UDF class**

The number in the interfaces (UDF1 to UDF22) signifies the number of parameters an UDF can take.

<note> Note: Currently, any UDF which can take more than 22 parameters is not supported. </note>

```
package some.package
import org.apache.spark.sql.api.java.UDF1

class StringLengthUDF extends UDF1[String, Int] {
 override def call(t1: String): Int = t1.length
}
```
<a id= create_udf> </a>
#### **Create the UDF Function**
After defining an UDF you can bundle the UDF class in a JAR file and create the function by using `./bin/snappy` of SnappyData. This creates a persistent entry in the catalog after which, you use the UDF.

```
CREATE FUNCTION udf_name AS qualified_class_name RETURNS data_type USING JAR '/path/to/file/udf.jar'
```
For example:
```
CREATE FUNCTION APP.strnglen AS some.package.StringLengthUDF RETURNS Integer USING JAR '/path/to/file/udf.jar'
[Rishi] we can modify the declaration section as above
```

You can write a JAVA or SCALA class to write an UDF implementation. 

<note>Note: For input/output types: 
</br>The framework always returns the Java types to the UDFs. So, if you are writing `scala.math.BigDecimal` as an input type or output type, an exception is reported. You can use `java.math.BigDecimal` in the SCALA code. </note>


**Return Types to UDF program type mapping **

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

#### **Use the UDF**

```
select strnglen(string_column) from <table>
```
If you try to use an UDF on a different type of column, for example, an **Int** column an exception is reported.


#### **Drop the Function**

```
DROP FUNCTION IF EXISTS udf_name
```

For example:

```
DROP FUNCTION IF EXISTS app.strnglen
```

#### **Modify an Existing UDF**

 1) Drop the existing UDF

 2) Modify the UDF code and [create a new UDF](#create_udf). You can create the UDF with the same name as that of the dropped UDF.


### Create User Defined Aggregate Functions

SnappyData uses same interface as that of Spark to define a User Defined Aggregate Function  `org.apache.spark.sql.expressions.UserDefinedAggregateFunction`. For more information refer to this [document](https://databricks.com/blog/2015/09/16/apache-spark-1-5-dataframe-api-highlights.html).
