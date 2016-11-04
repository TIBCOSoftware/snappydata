package org.apache.spark.sql.store

import io.snappydata.SnappyFunSuite
import io.snappydata.core.RefData

import io.snappydata.udf.UDF1

import org.apache.spark.sql.types.{DataTypes, DataType}

class StringLengthUDF extends UDF1[String, Int]{
  override def call(t1: String): Int = t1.length

  override def getDataType: DataType = DataTypes.IntegerType
}
class SnappyUDFTest extends SnappyFunSuite{

  test("Simple UDF"){
    val rdd = sc.parallelize((1 to 5).map(i => RefData(i, s"some $i")))
    val refDf = snc.createDataFrame(rdd)
    snc.sql("DROP TABLE IF EXISTS RR_TABLE")

    snc.sql("CREATE TABLE RR_TABLE(OrderRef INT NOT NULL, description String)")

    refDf.write.insertInto("RR_TABLE")

    snc.snappySession.createFunction("APP.stringLenghtTest",
      className = "org.apache.spark.sql.store.StringLengthUDF",
      isTemporary = false, None, None)


    val udfdf = snc.sql("select stringLenghtTest(description) from RR_TABLE")

    println(udfdf.queryExecution.executedPlan)

    udfdf.foreach(r => println(r))
  }

}
