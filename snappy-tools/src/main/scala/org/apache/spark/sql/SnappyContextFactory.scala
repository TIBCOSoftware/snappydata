package org.apache.spark.sql

import com.typesafe.config.Config
import spark.jobserver.context.SparkContextFactory

import org.apache.spark.SparkConf

/**
  * Created by soubhikc on 22/10/15.
  */
class SnappyContextFactory extends SparkContextFactory {

  type C = SnappyJobContext

  def makeContext(sparkConf: SparkConf, config: Config, contextName: String): C = {
    new SnappyJobContext(SnappyContext.globalSparkContext, SnappyContext.getOrCreate())
  }
}

object SnappyContextFactory {

}