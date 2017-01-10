package org.apache.spark.sql

import io.snappydata.SnappyFunSuite

import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.catalyst.expressions.{ScalaUDF, Literal}
import org.apache.spark.sql.internal.UDFFunction
import org.apache.spark.sql.types.DataTypes



class UDFunctionSuite extends SnappyFunSuite {

  ignore("test primitive Types") {
    /*var funcBuilder = UDFFunction.makeFunctionBuilder("name", new StringUDF().getClass)
    var scalaUDF = funcBuilder(Seq(Literal.create(null, DataTypes.StringType))).asInstanceOf[ScalaUDF]
    assert(scalaUDF.dataType == DataTypes.StringType)
    funcBuilder = UDFFunction.makeFunctionBuilder("name", new IntUDF().getClass)
    scalaUDF = funcBuilder(Seq(Literal.create(null, DataTypes.StringType))).asInstanceOf[ScalaUDF]
    assert(scalaUDF.dataType == DataTypes.IntegerType)*/
  }
}
