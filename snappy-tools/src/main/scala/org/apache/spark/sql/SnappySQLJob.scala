package org.apache.spark.sql

import spark.jobserver.{ContextLike, SparkJobBase}

import org.apache.spark.SparkContext

/**
 * Created by soubhikc on 22/10/15.
 */
trait SnappySQLJob  extends SparkJobBase {
  type C = SnappyJobContext
}

final class SnappyJobContext(val sc: SparkContext, val snc: SnappyContext) extends ContextLike {
  /**
    * The underlying SparkContext
    */
  override def sparkContext: SparkContext = sc

  /**
    * Responsible for performing any cleanup, including calling the underlying context's
    * stop method.
    */
  override def stop(): Unit = {
    // sparkContext.stop()
  }

  /**
    * Returns true if the job is valid for this context.
    * At the minimum this should check for if the job can actually take a context of this type;
    * for example, a SQLContext should only accept jobs that take a SQLContext.
    * The recommendation is to define a trait for each type of context job;  the standard
    * [[spark.jobserver.context.DefaultSparkContextFactory]] checks to
    * see if the job is of type [[spark.jobserver.SparkJob]].
    */
  override def isValidJob(job: SparkJobBase): Boolean = job.isInstanceOf[SnappySQLJob]
}
