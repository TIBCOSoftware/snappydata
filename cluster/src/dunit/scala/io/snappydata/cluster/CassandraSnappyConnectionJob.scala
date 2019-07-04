/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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
package io.snappydata.cluster

import com.typesafe.config.Config
import org.apache.spark.sql._
import org.junit.Assert

object CassandraSnappyConnectionJob extends SnappySQLJob {

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()

  override def runSnappyJob(sc: SnappySession, jobConfig: Config): Any = {

    // scalastyle:off println
    val df = sc.read.format("org.apache.spark.sql.cassandra").
        options(Map("table" -> "customer", "keyspace" -> "test")).load
    df.write.format("column").mode(SaveMode.Overwrite).saveAsTable("CUSTOMER")
    val showDF = sc.sql("select * from CUSTOMER")
    assert(showDF.count == 3, "Number of rows = " + showDF.count())
    assert(showDF.schema.fields.length == 4, "Number of columns = " + showDF.schema.fields.length)
  }
}
