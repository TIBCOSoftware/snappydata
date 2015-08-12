package org.apache.spark.streaming

import java.io.InputStream

import scala.reflect.ClassTag

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, SocketReceiver}
import org.apache.spark.util.Utils

/**
 * Encapsulates DDL operations on DStreams.
 *
 * Created by Hemant on 5/13/15.
 */
case class StreamRelation[T](
    dStream: DStream[T],
    options: Map[String, Any],
    formatter: (RDD[T], StructType) => RDD[Row],
    override val schema: StructType,
    @transient override val sqlContext: SQLContext)
    (implicit val ct: ClassTag[T])
    extends BaseRelation with TableScan with Logging {

  override def buildScan(): RDD[Row] =
    throw new IllegalAccessException("Take it easy boy!! It's a prototype. ")
}

final class StreamSource extends SchemaRelationProvider {

  override def createRelation(sqlContext: SQLContext,
      options: Map[String, String], schema: StructType) = {

    val addresses: String = OptsUtil.getOption(OptsUtil.SERVER_ADDRESS, options)
    val urls = addresses.split(",").map(addr => addr.split(":"))
    //val port: Int = OptsUtil.getOption(OptsUtil.PORT, options).toInt

    // Load the format function using reflection
    val interpreter = loadFormatClass(OptsUtil.getOption(
      OptsUtil.FORMAT, options)).newInstance() match {
      case f: UserDefinedInterpreter[_] =>
        // type erasure at runtime so [Any] is the type at this point
        // but actual classTag is available in interpreter.classTag
        f.asInstanceOf[UserDefinedInterpreter[Any]]
      case f => throw OptsUtil.newAnalysisException(s"Incorrect interpreter $f")
    }

    val storageLevel = OptsUtil.getOptionally(OptsUtil.STORAGE_LEVEL, options)
        .map(StorageLevel.fromString)
        .getOrElse(StorageLevel.MEMORY_AND_DISK_SER_2)

    val context = StreamingCtxtHolder.streamingContext
    val classTag = interpreter.classTag
    val converter = {
      input: InputStream => interpreter.converter(input, schema)
    }

    // Create a DStream here based on the parameters passed
    // as part of create stream
    val stream = context.union(urls.map { url =>
      context.withNamedScope("socket stream") {
        context.socketStream(url(0), url(1).toInt, converter,
          storageLevel)(classTag)
      }
    }.toSeq)

    val dstream = options.get(OptsUtil.WINDOWDURATION) match {
      case Some(wd) => options.get(OptsUtil.SLIDEDURATION) match {
        case Some(sd) => stream.window(Duration(wd.toInt), Duration(sd.toInt))
        case None => stream.window(Duration(wd.toInt))
      }
      case None => stream
    }

    StreamRelation(dstream, options, interpreter.formatter,
      schema, sqlContext)(classTag)
  }

  def loadFormatClass(provider: String): Class[_] = {
    val loader = Utils.getContextOrSparkClassLoader
    try {
      loader.loadClass(provider)
    } catch {
      case cnf: java.lang.ClassNotFoundException =>
        sys.error(s"Failed to load class for data source: $provider")
    }
  }
}

/**
 * User has to implement this trait to convert the InputStream to RDD[T]
 * and then format to RDD[Row].
 */
trait UserDefinedInterpreter[T] {

  def classTag: ClassTag[T]

  def converter(input: InputStream, schema: StructType): Iterator[T]

  def formatter(rdd: RDD[T], schema: StructType): RDD[Row]
}

/**
 * Extension to `UserDefinedInterpreter` for string streams.
 */
trait UserDefinedStringInterpreter extends UserDefinedInterpreter[String] {

  override final val classTag = scala.reflect.classTag[String]

  override def converter(input: InputStream, schema: StructType) =
    SocketReceiver.bytesToLines(input)
}
