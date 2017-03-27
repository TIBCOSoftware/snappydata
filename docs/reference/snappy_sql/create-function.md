#Create Function

```
CREATE [TEMPORARY] FUNCTION [db_name.]function_name AS class_name
    [USING resource, ...]

resource:
    : (JAR|FILE|ARCHIVE) file_uri
```

Create a function. The specified class for the function must extend either UDF or UDAF in `org.apache.hadoop.hive.ql.exec`, or one of `AbstractGenericUDAFResolver`, `GenericUDF`, or `GenericUDTF` in `org.apache.hadoop.hive.ql.udf.generic`. If a function with the same name already exists in the database, an exception will be thrown. 
	!!! Note: 
    	This command is supported only when Hive support is enabled.

**TEMPORARY**
The created function will be available only in this session and will not be persisted to the underlying metastore, if any. No database name may be specified for temporary functions.

**USING `<resources>`**
Specify the resources that must be loaded to support this function. A list of jar, file, or archive URIs may be specified. Known issue: adding jars does not work from the Spark shell (SPARK-8586).