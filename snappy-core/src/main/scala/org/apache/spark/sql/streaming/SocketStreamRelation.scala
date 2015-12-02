package org.apache.spark.sql.streaming

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.{DeletableRelation, DestroyRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
 * Created by ymahajan on 25/09/15.
 */

case class SocketStreamRelation[T](dStream: DStream[T],
                                   options: Map[String, String],
                                   formatter: (RDD[T], StructType) => RDD[Row],
                                   override val schema: StructType,
                                   @transient override val sqlContext: SQLContext)
                                  (implicit val ct: ClassTag[T])
  extends StreamBaseRelation with DeletableRelation
  with DestroyRelation with Logging with StreamPlan with Serializable {

  import scala.reflect.runtime.{universe => ru}

  private val streamToRow = {
    try {
      val clz = StreamUtils.loadClass(options("streamToRow"))
      clz.newInstance().asInstanceOf[MessageToRowConverter]
    } catch {
      case e: Exception => sys.error(s"Failed to load class : ${e.toString}")
    }
  }
  val clazz: Class[_] = streamToRow.getTargetType()
  val mirror = ru.runtimeMirror(clazz.getClassLoader)
  val sym = mirror.staticClass(clazz.getName)
  // obtain class symbol for `c`
  val tpe = sym.selfType
  // obtain type object for `c`
  val tt = ru.TypeTag[Product](mirror, new reflect.api.TypeCreator {
            def apply[U <: reflect.api.Universe with Singleton](m: reflect.api.Mirror[U]) = {
                assert(m eq mirror, s"TypeTag[$tpe] defined in $mirror cannot be migrated to $m.")
                tpe.asInstanceOf[U#Type]
            }
        })

  @transient val stream: DStream[InternalRow] = dStream.map(streamToRow.toRow)

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