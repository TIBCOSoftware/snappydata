package org.apache.spark.sql.store

import scala.util.{Failure, Success, Try}

import io.snappydata.SnappyFunSuite
import io.snappydata.core.RefData

import io.snappydata.udf.UDF1

import org.apache.spark.sql.types.{DataTypes, DataType}

class StringLengthUDF extends UDF1[String, Int]{
  override def call(t1: String): Int = t1.length

  override def getDataType: DataType = DataTypes.IntegerType
}
class SnappyUDFTest extends SnappyFunSuite{

  test("Test UDF API") {
    val rdd = sc.parallelize((1 to 5).map(i => RefData(i, s"some $i")))
    val refDf = snc.createDataFrame(rdd)
    snc.sql("DROP TABLE IF EXISTS RR_TABLE")

    snc.sql("CREATE TABLE RR_TABLE(OrderRef INT NOT NULL, description String)")

    refDf.write.insertInto("RR_TABLE")

    snc.snappySession.createFunction("APP.strnglen",
      className = "org.apache.spark.sql.store.StringLengthUDF",
      isTemporary = false)

    val query = s"select strnglen(description) from RR_TABLE"
    val udfdf = snc.sql(query)


    assert(udfdf.collect().forall(r => {
      r.getInt(0) == 6
    }))

    assert(snc.snappySession.sessionCatalog.listFunctions("app", "str*").
        find(f => (f._1.toString().contains("strnglen"))).size == 1)


    snc.snappySession.dropFunction("APP.strnglen", false, false)

    Try(snc.sql(query).count()) match {
      case Success(df) => throw new AssertionError(" Should not have succedded with dropped function")
      case Failure(error) => // Do nothing
    }
  }

  test("Test UDF SQL") {
    val rdd = sc.parallelize((1 to 5).map(i => RefData(i, s"some $i")))
    val refDf = snc.createDataFrame(rdd)
    snc.sql("DROP TABLE IF EXISTS RR_TABLE")

    snc.sql("CREATE TABLE RR_TABLE(OrderRef INT NOT NULL, description String)")

    refDf.write.insertInto("RR_TABLE")

    snc.snappySession.sql("CREATE FUNCTION APP.strnglen AS org.apache.spark.sql.store.StringLengthUDF")
    snc.snappySession.sql("CREATE FUNCTION APP.strlen AS demo1.StringLengthUDF USING JAR '/rishim1/snappy/snappy-commons/examples/build-artifacts/scala-2.11/classes/main/udf.jar'")



    val query = s"select strnglen(description) from RR_TABLE"
    val udfdf = snc.sql(query)
    val udf2 = snc.sql("select strlen(description) from RR_TABLE")
    assert(udf2.collect().forall(r => {
      r.getInt(0) == 6
    }))

    assert(udfdf.collect().forall(r => {
      r.getInt(0) == 6
    }))

    assert(snc.snappySession.sessionCatalog.listFunctions("app", "str*").
        find(f => (f._1.toString().contains("strnglen"))).size == 1)

    snc.snappySession.sql("DESCRIBE FUNCTION APP.strlen").collect().foreach(println);
    snc.snappySession.sql("DESCRIBE FUNCTION EXTENDED APP.strlen").collect().foreach(println);
    snc.snappySession.sql("DESCRIBE FUNCTION strlen").collect().foreach(println);
    snc.snappySession.sql("DESCRIBE FUNCTION EXTENDED strlen").collect().foreach(println);
    snc.snappySession.sql("SHOW FUNCTIONS strlen").collect().foreach(println);

    snc.snappySession.sql("DROP FUNCTION app.strnglen");
    snc.snappySession.sql("DROP FUNCTION IF EXISTS app.strlen");
    snc.snappySession.sql("DROP FUNCTION IF EXISTS app.strlen");

    Try(snc.sql(query).count()) match {
      case Success(df) => throw new AssertionError(" Should not have succedded with dropped function")
      case Failure(error) => // Do nothing
    }
  }

}
