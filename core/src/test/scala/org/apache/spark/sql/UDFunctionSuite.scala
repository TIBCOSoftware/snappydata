package org.apache.spark.sql

import io.snappydata.SnappyFunSuite

import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.catalyst.expressions.{ScalaUDF, Literal}
import org.apache.spark.sql.internal.UDFFunction
import org.apache.spark.sql.types.DataTypes

class StringUDF extends UDF1[String, String]{
  def call(name: String): String = {
    name
  }
}
class IntUDF extends UDF1[String, Int]{
  def call(name: String): Int = {
    name.length
  }
}

class IntUDF extends UDF1[String, Int]{
  def call(name: String): Int = {
    name.length
  }
}

class UDFunctionSuite extends SnappyFunSuite {

  test("test primitive Types") {
    var funcBuilder = UDFFunction.makeFunctionBuilder("name", new StringUDF().getClass)
    var scalaUDF = funcBuilder(Seq(Literal.create(null, DataTypes.StringType))).asInstanceOf[ScalaUDF]
    assert(scalaUDF.dataType == DataTypes.StringType)
    funcBuilder = UDFFunction.makeFunctionBuilder("name", new IntUDF().getClass)
    scalaUDF = funcBuilder(Seq(Literal.create(null, DataTypes.StringType))).asInstanceOf[ScalaUDF]
    assert(scalaUDF.dataType == DataTypes.IntegerType)
  }
}
