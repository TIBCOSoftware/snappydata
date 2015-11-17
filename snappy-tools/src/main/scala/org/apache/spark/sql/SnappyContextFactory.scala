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
    new SnappyJobContext(SnappyContext.globalContext, SnappyContext())
  }
}

object SnappyContextFactory {

}