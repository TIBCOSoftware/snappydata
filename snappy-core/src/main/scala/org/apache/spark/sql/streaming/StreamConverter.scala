package org.apache.spark.sql.streaming

import java.io.InputStream

trait StreamConverter extends Serializable {
  def convert(inputStream: InputStream): Iterator[Any]

  def getTargetType: scala.Predef.Class[_]
}

class MyStreamConverter extends StreamConverter with Serializable {
  override def convert(inputStream: java.io.InputStream): Iterator[Any] = {
    scala.io.Source.fromInputStream(inputStream, "UTF-8")
  }

  override def getTargetType: scala.Predef.Class[_] = classOf[String]
}