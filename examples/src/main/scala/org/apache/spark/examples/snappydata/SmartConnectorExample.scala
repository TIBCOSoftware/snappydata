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
package org.apache.spark.examples.snappydata

import org.apache.spark.sql.{SnappySession, SparkSession}

/**
 * This example shows how an application can interact with SnappyStore in Split cluster mode.
 * By this mode an application can access metastore of an existing running SnappyStore. Hence it can
 * query tables, write to tables which reside in a SnappyStore.
 *
 * To run this example you need to set up a Snappy Cluster first . To do the same, follow the steps
 * mentioned below.
 *
 * 1.  Go to SNAPPY_HOME. Your Snappy installation directory.
 *
 * 2.  Start a Snappy cluster
 * ./sbin/snappy-start-all.sh
 * This will start a simple cluster with one data node, one lead node and a locator
 *
 * 3.  Open Snappy Shell
 * ./bin/snappy-sql
 * This will open Snappy shell which can be used to create and query tables.
 *
 * 4. Connect to the Snappy Cluster. On the shell prompt type
 * connect client 'localhost:1527';
 *
 * 5. Create a column table and insert some rows in SnappyStore. Type the followings in Snappy Shell.
 *
 * CREATE TABLE SNAPPY_COL_TABLE(r1 Integer, r2 Integer) USING COLUMN;
 *
 * insert into SNAPPY_COL_TABLE VALUES(1,1);
 * insert into SNAPPY_COL_TABLE VALUES(2,2);
 *
 * 6. Run this example to see how this program interacts with the Snappy Cluster
 * table (SNAPPY_COL_TABLE) that we created. This program also creates a table in SnappyStore.
 * After running this example you can also query the table from Snappy shell
 * e.g. select count(*) from TestColumnTable.
 *
 * bin/run-example snappydata.SmartConnectorExample spark.snappydata.connection=localhost:1527
 *
 */

object SmartConnectorExample {

  def main(args: Array[String]): Unit = {

    val builder = SparkSession
      .builder
      .appName("SmartConnectorExample")
      // It can be any master URL
      .master("local[4]")

    args.foreach( prop => {
      val params = prop.split("=")
      builder.config(params(0), params(1))
    })

    val spark: SparkSession = builder
        .getOrCreate
    val snSession = new SnappySession(spark.sparkContext)

    println("\n\n ####  Reading from the SnappyStore table SNAPPY_COL_TABLE  ####  \n")
    val colTable = snSession.table("SNAPPY_COL_TABLE")
    colTable.show(10)


    println(" ####  Creating a table TestColumnTable  #### \n")

    snSession.dropTable("TestColumnTable", ifExists = true)

    // Creating a table from a DataFrame
    val dataFrame = snSession.range(1000).selectExpr("id", "floor(rand() * 10000) as k")

    snSession.sql("create table TestColumnTable (id bigint not null, k bigint not null) using column")

    dataFrame.write.insertInto("TestColumnTable")

    println(" ####  Write to table completed. ### \n\n" +
        "Now you can query table TestColumnTable using $SNAPPY_HOME/bin/snappy-shell")

  }

}
