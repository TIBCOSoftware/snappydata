# How to Store and Retrieve Complex Data Types in JDBC Programs

If you want to store/retrieve objects for complex data types (Array, Map and Struct) using JDBC programs, TIBCO ComputeDB provides **com.pivotal.gemfirexd.snappy.ComplexTypeSerializer** utility class to serialize/deserialize those objects:

*	A column of type **ARRAY** can store array of Java objects (Object[]), typed arrays, java.util.Collection, and scala.collection.Seq.
*	A column of type **MAP** can store java.util.Map or scala.collection.Map.
*	A column of type **STRUCT** can store array of Java objects (Object[]), typed arrays, java.util.Collection, scala.collection.Seq, or scala.Product.

!!! Note
	Complex data types are supported only for column tables.


## Code Example: Storing and Retrieving Array Data in a JDBC program

The following scala code snippets show how to perform insert and select operations on columns of complex data types. Complete source code for example is available at [JDBCWithComplexTypes.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/JDBCWithComplexTypes.scala) 

```
// create a JDBC connection
val url: String = s"jdbc:snappydata://localhost:1527/"
val conn = DriverManager.getConnection(url)

val stmt = conn.createStatement()
// create a table with a column of type array
stmt.execute("CREATE TABLE TABLE_WITH_COMPLEX_TYPES (col1 Int, col2 Array<Decimal>) USING column options()")
```

### Inserting Data

1.	Insert a single row having a complex type (array)

		val arrDecimal = Array(Decimal("4.92"), Decimal("51.98"))
		val pstmt = conn.prepareStatement("insert into TABLE_WITH_COMPLEX_TYPES values (?, ?)")

2.	Create a serializer that can be used to serialize array data and insert into the table.
    
        val serializer1 = ComplexTypeSerializer.create(tableName, "col2", conn)
        pstmt.setInt(1, 1)
        pstmt.setBytes(2, serializer1.serialize(arrDecimal))
        pstmt.execute
        pstmt.close()

### Selecting Data

```
// Select array data as a JSON string
 
var rs = stmt.executeQuery(s"SELECT * FROM $tableName")
while (rs.next()) {
 // read the column as a String
 val res1 = rs.getString("col2")
 println(s"res1 = $res1")
 // Alternate way, read the same column as a Clob
 val res2 = rs.getClob("col2")
 println(s"res2 = " + res2.getSubString(1, res2.length.asInstanceOf[Int]))
}

// Reading array data as  BLOB and Bytes and then forming a Scala array
val serializer = ComplexTypeSerializer.create(tableName, "col2", conn)
rs = stmt.executeQuery(s"SELECT * FROM $tableName --+ complexTypeAsJson(0)")
while (rs.next()) {
  // Read the same column as a byte[] and then deserialize it into an Array
  val res1 = serializer.deserialize(rs.getBytes("col2"))
  println(s"res1 = $res1")
  // alternate way, read the same column as a Blob an then deserialize it into an Array
  val res2 = serializer.deserialize(rs.getBlob("col2"))
  println(s"res2 = $res2")
}


var rs = stmt.executeQuery(s"SELECT * FROM $tableName")
while (rs.next()) {
 // read the column as a String
 val res1 = rs.getString("col2")
 println(s"res1 = $res1")
 // alternate way, read the same column as a Clob
 val res2 = rs.getClob("col2")
 println(s"res2 = " + res2.getSubString(1, res2.length.asInstanceOf[Int]))
}

// reading array data as  BLOB and Bytes and then forming a Scala array
val serializer = ComplexTypeSerializer.create(tableName, "col2", conn)
rs = stmt.executeQuery(s"SELECT * FROM $tableName --+ complexTypeAsJson(0)")
while (rs.next()) {
  // read the same column as a byte[] and then deserialize it into an Array
  val res1 = serializer.deserialize(rs.getBytes("col2"))
  println(s"res1 = $res1")
  // alternate way, read the same column as a Blob an then deserialize it into an Array
  val res2 = serializer.deserialize(rs.getBlob("col2"))
  println(s"res2 = $res2")
}


```

**See also:**</br>[How to connect using JDBC driver](/howto/connect_using_jdbc_driver.md)
