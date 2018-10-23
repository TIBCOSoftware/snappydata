/*
 * This is the test program.
 * a
 *
 */
package io.snappydata.v2.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType

/*
 * Test Program to for Data Source Testing.
 */
object SimpleSnappyStoreTest {

  /*
   *  Test Main Method
   */
  def main(args: Array[String]): Unit = {
      readDataFromSnappyStore()
  }

  /*
   * Single Partition In Memory Data Source. [Read operation]
   */
  def readDataFromSnappyStore(): Unit = {

    val builder = SparkSession.builder
      .appName("Test")
      .master("local[4]")

    val spark: SparkSession = builder.getOrCreate

    val dfReader = spark.read.format("io.snappydata.v2.connector.dummy.snappystore")
      .option("driver", "io.snappydata.jdbc.ClientDriver")
      .option("url", "jdbc:snappydata://localhost:1527/")
      .option("tableName", "AIRLINE")

    val df = dfReader.load()
      .select("YEAR_")
      .filter("YEAR_>2015")

    df.show(10)

    df.write.format("io.snappydata.v2.connector.dummy.snappystore")
        .option("driver", "io.snappydata.jdbc.ClientDriver")
        .option("url", "jdbc:snappydata://localhost:1527/")
        .option("tableName", "AIRLINE_1").save()


  }

}