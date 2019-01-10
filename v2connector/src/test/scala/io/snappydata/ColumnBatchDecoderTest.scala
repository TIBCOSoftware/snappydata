/*
 * Comment here
 */
package io.snappydata

import java.sql.DriverManager

import org.apache.spark.sql.SparkSession

object ColumnBatchDecoderTest {

  def main(args: Array[String]): Unit = {

    val builder = SparkSession
        .builder
        .appName("DecoderExample")
        .master("local[4]")

    builder.config("spark.snappydata.connection", "localhost:1527")

    args.foreach(prop => {
      val params = prop.split("=")
      builder.config(params(0), params(1))
    })

    val spark: SparkSession = builder.getOrCreate

    val conn = DriverManager.getConnection("jdbc:snappydata://localhost[1527]")
    val stmt = conn.createStatement()
    // stmt.execute("set snappydata.column.maxDeltaRows=1")
    stmt.execute("DROP TABLE IF EXISTS TEST_TABLE")
    stmt.execute("create table TEST_TABLE (ID long, rank int, designation String NULL ) " +
        "using column options (buckets '4', COLUMN_MAX_DELTA_ROWS '1') as select id, 101, " +
        " 'somerank' || id from range(20)")
    stmt.close()
    conn.close()

 /*
    val field1 = StructField("ID", LongType, true)
    val schema = new StructType(Array[StructField](field1, field2))
    val projection = new StructType(Array[StructField](field1, field2))


    for (bucketId <- 0 until 2) {
      // scan_colInput = (ColumnBatchIteratorOnRS)
      val columnBatchDecoderHelper = new V2ColumnBatchDecoderHelper(
        "APP.TEST_TABLE", projection, schema, null, bucketId,
        ArrayBuffer("127.0.0.1" ->
            "jdbc:snappydata://localhost[1528]/;route-query=false;load-balance=false"))

      columnBatchDecoderHelper.initialize
      while (columnBatchDecoderHelper.hasNext) {
        val columnBatchSpark = columnBatchDecoderHelper.next
        val iterator = columnBatchSpark.rowIterator()
        while (iterator.hasNext) {
          // scalastyle:off
          val row = iterator.next()
          println("Row " + row.getLong(0))
          println("Row " + row.getInt(1))
          // println("Row " + row.getInt(2))
        }
      }
    }
    */
  }
}
