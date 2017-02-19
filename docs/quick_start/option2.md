<a id="getting-started-using-spark-scala-apis"></a>
##Option 2: Getting Started Using Spark Scala APIs

**Create a SnappySession**: A SnappySession extends SparkSession so you can mutate data, get much higher performance, etc.

```scala
scala> val snappy = new org.apache.spark.sql.SnappySession(spark.sparkContext)
//Import snappy extensions
scala> import snappy.implicits._
```

**Create a small dataset using Spark's APIs**:
```scala
scala> val ds = Seq((1,"a"), (2, "b"), (3, "c")).toDS()
```
**Define a schema for the table**:
```scala
scala>  import org.apache.spark.sql.types._
scala>  val tableSchema = StructType(Array(StructField("CustKey", IntegerType, false),
          StructField("CustName", StringType, false)))
```

**Create a column table with a simple schema [String, Int] and default options**:
For detailed option refer to the [Row and Column Tables](../pgm_guide/tables_in_snappydata.md#tables-in-snappydata) section.
```scala
//Column tables manage data is columnar form and offer superier performance for analytic class queries.
scala>  snappy.createTable(tableName = "colTable",
          provider = "column", // Create a SnappyData Column table
          schema = tableSchema,
          options = Map.empty[String, String], // Map for options.
          allowExisting = false)
```
SnappyData (SnappySession) extends SparkSession, so you can simply use all the Spark's APIs.

**Insert the created DataSet to the column table "colTable"**:
```scala
scala>  ds.write.insertInto("colTable")
// Check the total row count.
scala>  snappy.table("colTable").count
```
**Create a row object using Spark's API and insert the Row to the table**:
Unlike Spark DataFrames SnappyData column tables are mutable. You can insert new rows to a column table.

```scala
// Insert a new record
scala>  import org.apache.spark.sql.Row
scala>  snappy.insert("colTable", Row(10, "f"))
// Check the total row count after inserting the row.
scala>  snappy.table("colTable").count
```

**Create a "row" table with a simple schema [String, Int] and default options**: 
For detailed option refer to the [Row and Column Tables](../pgm_guide/tables_in_snappydata.md#tables-in-snappydata) section.

```scala
//Row formatted tables are better when datasets constantly change or access is selective (like based on a key).
scala>  snappy.createTable(tableName = "rowTable",
          provider = "row",
          schema = tableSchema,
          options = Map.empty[String, String],
          allowExisting = false)
```

**Insert the created DataSet to the row table "rowTable"**:
```scala
scala>  ds.write.insertInto("rowTable")
//Check the row count
scala>  snappy.table("rowTable").count
```
**Insert a new record**:
```scala
scala>  snappy.insert("rowTable", Row(4, "d"))
//Check the row count now
scala>  snappy.table("rowTable").count
```

**Change some data in a row table**:
```scala
//Updating a row for customer with custKey = 1
scala>  snappy.update(tableName = "rowTable", filterExpr = "CUSTKEY=1",
                newColumnValues = Row("d"), updateColumns = "CUSTNAME")

scala>  snappy.table("rowTable").orderBy("CUSTKEY").show

//Delete the row for customer with custKey = 1
scala>  snappy.delete(tableName = "rowTable", filterExpr = "CUSTKEY=1")

```

```scala
//Drop the existing tables
scala>  snappy.dropTable("rowTable", ifExists = true)
scala>  snappy.dropTable("colTable", ifExists = true)
```