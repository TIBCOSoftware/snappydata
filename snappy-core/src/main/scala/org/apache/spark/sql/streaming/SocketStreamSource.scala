package org.apache.spark.sql.streaming

import org.apache.spark.sql.sources.SchemaRelationProvider
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream

/**
 * Created by ymahajan on 25/09/15.
 */

final class SocketStreamSource extends SchemaRelationProvider {
  override def createRelation(sqlContext: SQLContext,
                              options: Map[String, String], schema: StructType) = {

    val hostname: String = options.get("hostname").getOrElse("localhost")

    val port: Int = options.get("port").map(_.toInt).getOrElse(9999)
    //TODO: Yogesh, revisit these defaults

    val storageLevel = options.get("storageLevel")
      .map(StorageLevel.fromString)
      .getOrElse(StorageLevel.MEMORY_AND_DISK_SER_2)

    import scala.reflect.runtime.{universe => ru}

    val formatter = StreamUtils.loadClass(options("formatter")).newInstance() match {
      case f: StreamFormatter[_] => f.asInstanceOf[StreamFormatter[Any]]
      case f => throw new AnalysisException(s"Incorrect StreamFormatter $f")
    }

    val context = StreamingCtxtHolder.streamingContext
    val CONVERTER = "converter"

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
      val dStream = context.socketStream(hostname, port, converter.convert,
        storageLevel)

      SocketStreamRelation(dStream, options, formatter.format,
        schema, sqlContext)(formatter.classTag)
    }
    else {
      val dStream = context.socketTextStream(hostname, port,
        storageLevel)
      SocketStreamRelation(dStream.asInstanceOf[DStream[Any]], options,
        formatter.format, schema, sqlContext)(formatter.classTag)
    }
  }
}