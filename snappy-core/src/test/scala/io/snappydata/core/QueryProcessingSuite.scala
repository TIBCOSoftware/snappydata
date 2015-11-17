package io.snappydata.core

import org.scalatest.{Ignore, BeforeAndAfter, FunSuite}
import org.apache.spark.sql.types._
import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.sql.Row
import org.apache.spark.sql.snappy._

/**
 * Created by Suyog Bhokare on 2/11/15.
 */
class QueryProcessingSuite extends FunSuite with BeforeAndAfter with Logging {

    var conf = new SparkConf().setAppName("Simple Application").setMaster("local[1]")
    conf.set("spark.sql.unsafe.enabled", "false")
    var sc = new org.apache.spark.SparkContext(conf)
    var snContext = org.apache.spark.sql.SnappyContext(sc)
    // Create an RDD
    var people = Seq(Seq("John", 30), Seq("Tora", 31), Seq("Mark", 40), Seq("Rob", 40), Seq("Tommy", 40), Seq("Tod", 42))
    var sample_people = Seq(Seq("John", 30), Seq("Tommy", 40), Seq("Tod", 42))
    var sample_people_1 = Seq(Seq("Tora", 32),Seq("Tommy", 40), Seq("Tod", 42))
    var expected = ""


  ignore("Run a avg query on sample table if pre query triage qualifies the query to run on sample table") {

   // Generate the schema based on the string of schema
   // val schemaTypes = List(StringType, IntegerType)
   // val schema = StructType(schemaString.split(" ").zipWithIndex.map(
   // { case (fieldName, i) => StructField(fieldName, schemaTypes(i), true) }
   // ))

    // Convert records of the RDD (people) to Rows.
    val rowRDD = sc.parallelize(people, people.length).map(s =>
      new DataX(s(0).asInstanceOf[String], s(1).asInstanceOf[Int]))
    val sample_rowRDD = sc.parallelize(sample_people, sample_people.length).map(s =>
      new DataX(s(0).asInstanceOf[String], s(1).asInstanceOf[Int]))

    // Apply the schema to the RDD.
    val peopleDataFrame = snContext.createDataFrame(rowRDD)
    val sample_peopleDataFrame = snContext.createDataFrame(sample_rowRDD)

    // Register the DataFrames as a table.
    peopleDataFrame.registerTempTable("people")

    snContext.registerSampleTable("people_sampled",
      sample_peopleDataFrame.schema, Map(
        "qcs" -> "age",
        "fraction" -> 0.01,
        "strataReservoirSize" -> 50), Some("people"))

    sample_peopleDataFrame.insertIntoSampleTables("people_sampled")

    //Run query on actual table
    val result = snContext.sql("SELECT AVG(age) FROM people where age > 35")
    expected = "[40.5]"
    result.collect.foreach(verifyResult)

    // Run query on sample table
    val sampled_result = snContext.sql("SELECT AVG(age) FROM people where age > 35 ERRORPERCENT 5" )
    expected = "[41.0]"
    sampled_result.collect.foreach(verifyResult)
    println("Success...Test1")
  }

  test("Run a sum query on sample table if pre query triage qualifies the query to run on sample table") {

    // Generate the schema based on the string of schema
    // val schemaTypes = List(StringType, IntegerType)
    // val schema = StructType(schemaString.split(" ").zipWithIndex.map(
    // { case (fieldName, i) => StructField(fieldName, schemaTypes(i), true) }
    // ))

    // Convert records of the RDD (people) to Rows.
    val rowRDD = sc.parallelize(people, people.length).map(s =>
      new DataX(s(0).asInstanceOf[String], s(1).asInstanceOf[Int]))
    val sample_rowRDD = sc.parallelize(sample_people, sample_people.length).map(s =>
      new DataX(s(0).asInstanceOf[String], s(1).asInstanceOf[Int]))

    // Apply the schema to the RDD.
    val peopleDataFrame = snContext.createDataFrame(rowRDD)
    val sample_peopleDataFrame = snContext.createDataFrame(sample_rowRDD)

    // Register the DataFrames as a table.
    peopleDataFrame.registerTempTable("people")

    snContext.registerSampleTable("people_sampled",
      sample_peopleDataFrame.schema, Map(
        "qcs" -> "age",
        "fraction" -> 0.01,
        "strataReservoirSize" -> 50), Some("people"))

    sample_peopleDataFrame.insertIntoSampleTables("people_sampled")

    //Run query on actual table
    val result = snContext.sql("SELECT sum(age) FROM people where age > 35")
    expected = "[162]"
    result.collect.foreach(verifyResult)

    // Run query on sample table
    val sampled_result = snContext.sql("SELECT sum(age) FROM people where age > 35 ERRORPERCENT 5" )
    expected = "82.0"
    sampled_result.collect.foreach(verifyResult)
    println("Success...Test1")
  }


  ignore("Run query on appropriate sample table if multiple sample tables for a sample table available") {

    val sample_rowRDD = sc.parallelize(sample_people_1, sample_people_1.length).map(s =>
      new DataX(s(0).asInstanceOf[String], s(1).asInstanceOf[Int]))

    val sample_peopleDataFrame1 = snContext.createDataFrame(sample_rowRDD)

    snContext.registerSampleTable("people_sampled_1",
      sample_peopleDataFrame1.schema, Map(
        "qcs" -> "age,name",
        "fraction" -> 0.01,
        "strataReservoirSize" -> 50), Some("people"))

    sample_peopleDataFrame1.insertIntoSampleTables("people_sampled_1")

    // Run query on appropriate sample table ( Expected to execute on people_sampled_1 table)
    val sampled_result = snContext.sql("SELECT AVG(age) FROM people where age > 30 AND name like 'To%' ERRORPERCENT 5")
    expected = "[38.0]"
    sampled_result.collect.foreach(verifyResult)
    println("Success...Test2")
  }

  def verifyResult(r: Row): Unit = {
    println(r.toString())
    val x =  if(r.schema(0).dataType.isInstanceOf[StructType]) {
       val struct = r.getStruct(0)
      struct.get(0).toString
    } else {
       r.toString
    }
    assert(x == expected)
  }

}

case class DataX(name: String, age: Int) extends Serializable