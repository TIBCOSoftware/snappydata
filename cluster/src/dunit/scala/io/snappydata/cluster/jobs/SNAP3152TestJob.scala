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
package io.snappydata.cluster.jobs

import com.typesafe.config.Config
import org.apache.spark.sql._

object SNAP3152TestJob extends SnappySQLJob {

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()

  override def runSnappyJob(sc: SnappySession, jobConfig: Config): Any = {
    // scalastyle:off println

    // test for SNAP-3152 with deploy package
    println("test for SNAP-3152 with deploy package")
    assert(sc.sql("list packages").count() == 0)
    sc.sql("drop table if exists customer11")
    sc.sql("deploy package cassandraJar " +
        "'com.datastax.spark:spark-cassandra-connector_2.11:2.0.7'")
    println("Deployed successfully")
    assert(sc.sql("list packages").count() == 1)
    sc.sql("create external table customer11 " +
        "using org.apache.spark.sql.cassandra options" +
        " (table 'customer', keyspace 'test', spark.cassandra.input.fetch.size_in_rows '200000'," +
        " spark.cassandra.read.timeout_ms '10000')")
    assert(sc.sql("select * from customer11").count() == 3)
    sc.sql("undeploy cassandraJar")
    assert(sc.sql("list packages").count() == 0)
    println("Undeployed successfully")
    sc.sql("drop table customer11")


    // test for SNAP-3152 with deploy jar
    println("test for SNAP-3152 with deploy jar")
    val connectorJarLoc = jobConfig.getString("connectorJar")
    sc.sql(s"deploy jar cassJar '$connectorJarLoc'")
    println("Deployed successfully")
    assert(sc.sql("list packages").count() == 1)
    sc.sql("create external table customer12 " +
        "using org.apache.spark.sql.cassandra options" +
        " (table 'customer', keyspace 'test', spark.cassandra.input.fetch.size_in_rows '200000'," +
        " spark.cassandra.read.timeout_ms '10000')")
    assert(sc.sql("select * from customer12").count() == 3)
    sc.sql("undeploy cassJar")
    assert(sc.sql("list packages").count() == 0)
    println("Undeployed successfully")
    sc.sql("drop table customer12")

  }
}
