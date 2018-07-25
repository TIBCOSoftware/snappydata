/*
 * Test Class for dummy data source implementation
 * which reads data from static array.
 *
 */

import org.apache.spark.sql.SparkSession

object SimpleMemoryArrayTest {

  def main(args: Array[String]): Unit = {
    multiPartitionInMemory()
  }

  /*
    * Implementation of the Spark Data Source v2
    * which support reading data from the static array.
    *
    */
  def multiPartitionInMemory(): Unit = {

    val builder = SparkSession.builder
      .appName("Test")
      .master("local[4]")

    val spark: SparkSession = builder.getOrCreate

    val simpleDf = spark.read
      .format("io.snappydata.v2.connector.dummy.array")
      .load()

    simpleDf.show()

    // scalastyle:off
    println("number of partitions in simple source is " + simpleDf.rdd.getNumPartitions)

  }
}