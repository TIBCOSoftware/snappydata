<a id="getting-started-using-spark-scala-apis"></a>
# Using Spark Scala APIs

1. **Create a Snappy Session**</br>
    SnappySession extends the SparkSession so you can mutate data, get much higher performance, etc.

        scala> val snappy = new org.apache.spark.sql.SnappySession(spark.sparkContext)
        // Import Snappy extensions
        scala> import snappy.implicits._

2. **Create a dataset using the Spark APIs**</br>

		scala> val ds = Seq((1,"a"), (2, "b"), (3, "c")).toDS()

3. **Define a schema for the table**</br>

        scala>  import org.apache.spark.sql.types._
        scala>  val tableSchema = StructType(Array(StructField("CustKey", IntegerType, false),
                  StructField("CustName", StringType, false)))

4. **Create a "column" table with a simple schema [String, Int] and default options**</br>
    For detailed option refer to the [Row and Column Tables](../programming_guide/tables_in_snappydata.md#row-and-column-tables) section.

        // Column tables manage data is columnar form and offer superior performance for analytic class queries.
        scala>  snappy.createTable(tableName = "colTable",
                  provider = "column", // Create a TIBCO ComputeDB Column table
                  schema = tableSchema,
                  options = Map.empty[String, String], // Map for options
                  allowExisting = false)

    TIBCO ComputeDB (Snappy Session) extends SparkSession, so you can simply use all the Spark's APIs.

5. **Insert the created DataSet to the column table "colTable"**</br>

        scala>  ds.write.insertInto("colTable")
        // Check the total row count.
        scala>  snappy.table("colTable").count

6. **Create a row object using Spark's API and insert the row into the table**</br>
	Unlike Spark DataFrames TIBCO ComputeDB column tables are mutable. You can insert new rows to a column table.

        // Insert a new record
        scala>  import org.apache.spark.sql.Row
        scala>  snappy.insert("colTable", Row(10, "f"))
        // Check the total row count after inserting the row
        scala>  snappy.table("colTable").count

7. **Create a "row" table with a simple schema [String, Int] and default options** </br>
	For detailed option refer to the [Row and Column Tables](../programming_guide/tables_in_snappydata.md#row-and-column-tables) section.

        // Row formatted tables are better when datasets constantly change or access is selective (like based on a key)
        scala>  snappy.createTable(tableName = "rowTable",
        provider = "row",
        schema = tableSchema,
        options = Map.empty[String, String],
        allowExisting = false)

8. **Insert the created DataSet to the row table "rowTable"**</br>

        scala>  ds.write.insertInto("rowTable")
        // Check the row count
        scala>  snappy.table("rowTable").count

9. **Insert a new record**</br>

        scala>  snappy.insert("rowTable", Row(4, "d"))
        //Check the row count now
        scala>  snappy.table("rowTable").count


10. **Change some data in the row table**</br>

        // Updating a row for customer with custKey = 1
        scala>  snappy.update(tableName = "rowTable", filterExpr = "CUSTKEY=1",
                        newColumnValues = Row("d"), updateColumns = "CUSTNAME")

        scala>  snappy.table("rowTable").orderBy("CUSTKEY").show

        // Delete the row for customer with custKey = 1
        scala>  snappy.delete(tableName = "rowTable", filterExpr = "CUSTKEY=1")

        // Drop the existing tables
        scala>  snappy.dropTable("rowTable", ifExists = true)
        scala>  snappy.dropTable("colTable", ifExists = true)

