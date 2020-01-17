/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.snappydata.cluster.jobs

import com.typesafe.config.Config

import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

object SNAP3028TestJob extends SnappySQLJob {
  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation =
    SnappyJobValid()

  override def runSnappyJob(snappy: SnappySession, jobConfig: Config): Any = {
    snappy.sql("create table users (id long, name string) using column")
    try {
      snappy.sql("insert into users values (1, 'name1')")
      println("Job strted:")
      for(i <- 0 to 3) {
        val start = System.nanoTime()
        val df = snappy.table("users")
        import snappy.implicits._
        df.as[User].collectAsList()
        println("Time taken:" + (System.nanoTime() - start))
      }
    } finally {
      snappy.sql("drop table users")
    }
  }
}

case class User(id: Long, name: String)