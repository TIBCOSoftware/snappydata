package org.apache.spark.sql

import com.typesafe.config.Config
import org.apache.spark.{SparkConf, SparkContext}
import spark.jobserver.context.SparkContextFactory
import spark.jobserver.{ContextLike, SparkJobBase}

/**
  * Created by soubhikc on 22/10/15.
  */
class SnappyContextFactory extends SparkContextFactory {

  type C = SnappyJobContext

  def makeContext(sparkConf: SparkConf, config: Config, contextName: String): C = {
    new SnappyJobContext(SnappyContext.globalContext, SnappyContextFactory.snappyContext)
  }
}

object SnappyContextFactory {
  @volatile private[this] var _snc: SnappyContext = _

  def setSnappyContext(sc: SnappyContext): Unit = this.synchronized {
    _snc = sc
  }

  def snappyContext: SnappyContext = _snc
}