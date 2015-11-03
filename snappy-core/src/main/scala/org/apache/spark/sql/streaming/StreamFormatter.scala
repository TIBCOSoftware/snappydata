package org.apache.spark.sql.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import scala.reflect.ClassTag

/**
 * Created by ymahajan on 25/09/15.
 */
trait StreamFormatter[T] {
  def format(rdd: RDD[T], schema: StructType): RDD[Row]

  //def getTargetType() : scala.Predef.Class[_]
  def classTag: ClassTag[T]
}

class MyStreamFormatter extends StreamFormatter[String] {

  override final val classTag = scala.reflect.classTag[String]

  override def format(rdd: RDD[String], schema: StructType): RDD[Row] = {
     rdd.map(p => {
      val s = p.split(",")
      Row.fromTuple("A", "B", "C", "D", s(0))
    })
  }
}
