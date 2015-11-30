package org.apache.spark.sql.streaming

import java.io.InputStream

/**
 * Created by ymahajan on 25/09/15.
 */
trait StreamConverter {
  def convert(inputStream: InputStream): Iterator[Any]

  def getTargetType(): scala.Predef.Class[_]
}

class MyStreamConverter extends StreamConverter {
  override def convert(inputStream: java.io.InputStream): Iterator[Any] = {
    scala.io.Source.fromInputStream(inputStream, "UTF-8")
  }

  override def getTargetType = classOf[String]
}