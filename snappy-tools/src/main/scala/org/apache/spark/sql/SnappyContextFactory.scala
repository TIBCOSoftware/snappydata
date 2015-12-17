package org.apache.spark.sql

import com.typesafe.config.Config
import io.snappydata.impl.LeadImpl
import spark.jobserver.context.SparkContextFactory
import spark.jobserver.{ContextLike, SparkJobBase}

import org.apache.spark.SparkConf


/**
  * Created by soubhikc on 22/10/15.
  */
trait SnappySQLJob extends SparkJobBase {
  type C = SnappyContext
}


/**
  * Created by soubhikc on 22/10/15.
  */
class SnappyContextFactory extends SparkContextFactory {

  type C = SnappyContext with ContextLike

  def makeContext(sparkConf: SparkConf, config: Config, contextName: String): C = {
    new SnappyContext(LeadImpl.getInitializingSparkContext()) with ContextLike {
      override def isValidJob(job: SparkJobBase): Boolean = job.isInstanceOf[SnappySQLJob]
      override def stop(): Unit = {
        // not stopping anything here because of singleton nature.
      }
    }
  }
}

