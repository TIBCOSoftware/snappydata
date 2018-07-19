/*
 * This is the test program.
 * a
 *
 */
package io.snappydata.v2.test

;

import org.apache.spark.sql.SparkSession

/*
 * Test Program to for Data Source Testing.
 */
object Test {

  /*
   *  Test Main Method
   */
  def main(args: Array[String]): Unit = {

      singlePartitionInMemory()

      // multiPartitionInMemory()
  }


  /*
   * Single Partition In Memory Data Source. [Read operation]
   */
  def singlePartitionInMemory(): Unit = {

    val builder = SparkSession.builder
      .appName("SmartConnectorExample")
      .master("local[4]")

    val spark: SparkSession = builder.getOrCreate

    val simpleDf = spark.read
      .format("io.snappydata.v2.singlepart")
      .load()

    simpleDf.filter("value=2")

    simpleDf.show()

    // scalastyle:off
    println("number of partitions in simple source is " + simpleDf.rdd.getNumPartitions)

  }

  /*
   * Single Partition In Memory Data Source. [Read operation]
   */
  def multiPartitionInMemory(): Unit = {

    val builder = SparkSession.builder
      .appName("SmartConnectorExample")
      .master("local[4]")

    val spark: SparkSession = builder.getOrCreate

    val simpleDf = spark.read
      .format("io.snappydata.v2.multipart")
      .load()

    simpleDf.show()

    // scalastyle:off
    println("number of partitions in simple source is " + simpleDf.rdd.getNumPartitions)

  }
}