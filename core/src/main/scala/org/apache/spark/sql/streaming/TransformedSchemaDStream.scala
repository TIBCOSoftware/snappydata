/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package org.apache.spark.sql.streaming

import scala.reflect.ClassTag

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Time}

private[streaming]
class TransformedSchemaDStream[U: ClassTag](
    parent: SchemaDStream,
    parents: Seq[DStream[_]],
    transformFunc: (Seq[RDD[_]], Time) => RDD[U]
) extends DStream[U](parent.snsc) {

  require(parents.nonEmpty, "List of DStreams to transform is empty")
  require(parents.map(_.slideDuration).distinct.size == 1,
    "Some of the DStreams have different slide durations")

  override def dependencies: List[DStream[_]] = parents.toList

  override def slideDuration: Duration = parents.head.slideDuration

  override def compute(validTime: Time): Option[RDD[U]] = {
    val parentRDDs = parents.map { parent => parent.compute(validTime).getOrElse(
      // Guard out against parent DStream that return None instead of Some(rdd) to avoid NPE
      throw new SparkException(s"Couldn't generate RDD from parent at time $validTime"))
    }
    val transformedRDD = transformFunc(parentRDDs, validTime)
    if (transformedRDD == null) {
      throw new SparkException("Transform function must not return null. " +
          "Return SparkContext.emptyRDD() instead to represent no element " +
          "as the result of transformation.")
    }
    Some(transformedRDD)
  }
}
