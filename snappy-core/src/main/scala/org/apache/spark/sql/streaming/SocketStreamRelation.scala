package org.apache.spark.sql.streaming

import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.{DeletableRelation, DestroyRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream

/**
 * Created by ymahajan on 25/09/15.
 */

case class SocketStreamRelation(@transient override val sqlContext: SQLContext,
                                options: Map[String, String],
                                override val schema: StructType)
  extends StreamBaseRelation with DeletableRelation
  with DestroyRelation with Logging with StreamPlan with Serializable {

  val hostname: String = options.get("hostname").get //.getOrElse("localhost")

  val port: Int = options.get("port").map(_.toInt).get //.getOrElse(9999)
  //TODO: Yogesh, revisit these defaults

  val storageLevel = options.get("storageLevel")
    .map(StorageLevel.fromString)
    .getOrElse(StorageLevel.MEMORY_AND_DISK_SER_2)

  import scala.reflect.runtime.{universe => ru}

  @transient val context = StreamingCtxtHolder.streamingContext
  val CONVERTER = "converter"

  @transient private val socketStream =
    if (options.exists(_._1 == CONVERTER)) {
      val converter = StreamUtils.loadClass(options(CONVERTER)).newInstance().asInstanceOf[StreamConverter]
      val clazz: Class[_] = converter.getTargetType()
      val mirror = ru.runtimeMirror(clazz.getClassLoader)
      val sym = mirror.staticClass(clazz.getName) // obtain class symbol for `c`
      val tpe = sym.selfType // obtain type object for `c`
      val tt = ru.TypeTag[Product](mirror, new reflect.api.TypeCreator {
            def apply[U <: reflect.api.Universe with Singleton](m: reflect.api.Mirror[U]) = {
                assert(m eq mirror, s"TypeTag[$tpe] defined in $mirror cannot be migrated to $m.")
                tpe.asInstanceOf[U#Type]
            }
        })
      context.socketStream(hostname, port, converter.convert,
        storageLevel)
    }
    else {
      context.socketTextStream(hostname, port,
        storageLevel)
    }

  private val streamToRow = {
    try {
      val clz = StreamUtils.loadClass(options("streamToRow"))
      clz.newInstance().asInstanceOf[MessageToRowConverter]
    } catch {
      case e: Exception => sys.error(s"Failed to load class : ${e.toString}")
    }
  }

  //  val clazz: Class[_] = streamToRow.getTargetType()
  //  val mirror = ru.runtimeMirror(clazz.getClassLoader)
  //  val sym = mirror.staticClass(clazz.getName)
  //  // obtain class symbol for `c`
  //  val tpe = sym.selfType
  //  // obtain type object for `c`
  //  val tt = ru.TypeTag[Product](mirror, new reflect.api.TypeCreator {
  //            def apply[U <: reflect.api.Universe with Singleton](m: reflect.api.Mirror[U]) = {
  //                assert(m eq mirror, s"TypeTag[$tpe] defined in $mirror cannot be migrated to $m.")
  //                tpe.asInstanceOf[U#Type]
  //            }
  //        })

  @transient val stream: DStream[InternalRow] = {
    socketStream.map(streamToRow.toRow)
  }

  override def destroy(ifExists: Boolean): Unit = {
    throw new IllegalAccessException("Stream tables cannot be dropped")
  }

  override def delete(filterExpr: String): Int = {
    throw new IllegalAccessException("Stream tables cannot be dropped")
  }

  def truncate(): Unit = {
    throw new IllegalAccessException("Stream tables cannot be truncated")
  }
}