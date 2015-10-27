package io.snappydata.jobserver

import com.typesafe.config.Config
import io.snappydata.SnappySQLJob
import spark.jobserver.context.SparkContextFactory
import spark.jobserver.{ContextLike, SparkJobBase}

import org.apache.spark.sql.SnappyContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by soubhikc on 22/10/15.
 */
class SnappyContextFactory extends SparkContextFactory {

  type C = SnappyContext with ContextLike

  def makeContext(sparkConf: SparkConf, config: Config, contextName: String): C = {
    val sc = SnappyContextFactory.getOrElse(new SparkContext(sparkConf))
    new SnappyContext(sc) with ContextLike {
      def isValidJob(job: SparkJobBase): Boolean = job.isInstanceOf[SnappySQLJob]

      def stop(): Unit = {
        sparkContext.stop()
        SnappyContextFactory.clear()
      }
    }
  }
}

object SnappyContextFactory {

  @volatile private[this] var sparkContextRef: SparkContext = _

  def getOrElse(create: => SparkContext) = {
    val sc = sparkContextRef
    if (sc != null) {
      sc
    }
    else this.synchronized {
      val sc = sparkContextRef
      if (sc != null) {
        sc
      } else {
        sparkContextRef = create
        sparkContextRef
      }
    }
  }

  def clear() = this.synchronized {
    sparkContextRef = null
  }

  def sparkContext(): Option[SparkContext] = {
    val sc = sparkContextRef
    if (sc != null) Some(sc) else None
  }
}
