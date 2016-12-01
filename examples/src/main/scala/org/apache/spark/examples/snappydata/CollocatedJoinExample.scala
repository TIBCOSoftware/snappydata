/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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

import java.io.PrintWriter

import com.typesafe.config.Config
import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql.{SnappySession, SparkSession, SnappyJobValid, SnappyJobValidation, SnappyContext, SnappySQLJob}

/**
 * An example that shows how to join between collocated tables
 *
 * <p></p>
 * This example can be run either in local mode(in which it will spawn a single
 * node SnappyData system) or can be submitted as a job to an already running
 * SnappyData cluster.
 *
 * <p></p>
 * To run the example in local mode go to your SnappyData product distribution
 * directory and type following command on the command prompt
 * <pre>
 * bin/run-example snappydata.CollocatedJoinExample
 * </pre>
 *
 * To submit this example as a job to an already running cluster
 * <pre>
 *   cd $SNAPPY_HOME
 *   bin/snappy-job.sh submit
 *   --app-name CollocatedJoinExample
 *   --class org.apache.spark.examples.snappydata.CollocatedJoinExample
 *   --app-jar examples/jars/quickstart.jar
 *   --lead [leadHost:port]
 *
 * Check the status of your job id
 * bin/snappy-job.sh status --lead [leadHost:port] --job-id [job-id]
 *
 * The output of the job will be redirected to a file named CollocatedJoinExample.out
 */
object CollocatedJoinExample extends SnappySQLJob {

  override def runSnappyJob(snc: SnappyContext, jobConfig: Config): Any = {
    val pw = new PrintWriter("CollocatedJoinExample.out")
    runCollocatedJoinQuery(snc, pw)
    pw.close()
  }

  override def isValidJob(sc: SnappyContext, config: Config): SnappyJobValidation = SnappyJobValid()

  def runCollocatedJoinQuery(snc: SnappyContext, pw: PrintWriter): Unit = {
    pw.println()

    pw.println("****Collocated Join Example****")

    pw.println("Creating a column table(CUSTOMER)")

    snc.sql("DROP TABLE IF EXISTS CUSTOMER")

    // "PARTITION_BY" attribute specifies partitioning key for CUSTOMER table(C_CUSTKEY),
    // Refer to the documentation, for complete list of attributes
    snc.sql("CREATE TABLE CUSTOMER ( " +
        "C_CUSTKEY     INTEGER NOT NULL," +
        "C_NAME        VARCHAR(25) NOT NULL," +
        "C_ADDRESS     VARCHAR(40) NOT NULL," +
        "C_NATIONKEY   INTEGER NOT NULL," +
        "C_PHONE       VARCHAR(15) NOT NULL," +
        "C_ACCTBAL     DECIMAL(15,2)   NOT NULL," +
        "C_MKTSEGMENT  VARCHAR(10) NOT NULL," +
        "C_COMMENT     VARCHAR(117) NOT NULL)" +
        "USING COLUMN OPTIONS (PARTITION_BY 'C_CUSTKEY')")

    snc.sql("INSERT INTO CUSTOMER VALUES(20000, 'Customer20000', " +
        "'Chicago, IL', 1, '555-101-782', 3500, 'MKTSEGMENT', '')")
    snc.sql("INSERT INTO CUSTOMER VALUES(30000, 'Customer30000', " +
        "'Boston, MA', 1, '555-151-678', 4500, 'MKTSEGMENT', '')")
    snc.sql("INSERT INTO CUSTOMER VALUES(40000, 'Customer40000', " +
        "'San Jose, CA', 1, '555-532-345', 5500, 'MKTSEGMENT', '')")

    pw.println()
    pw.println("Creating a ORDERS table collocated with CUSTOMER")
    snc.sql("DROP TABLE IF EXISTS ORDERS")

    // "PARTITION_BY" attribute specifies partitioning key for ORDERS table(O_ORDERKEY),
    // "COLOCATE_WITH" specifies that the table is colocated with CUSTOMERS table
    // Refer to the documentation, for complete list of attributes
    snc.sql("CREATE TABLE ORDERS  ( " +
        "O_ORDERKEY       INTEGER NOT NULL," +
        "O_CUSTKEY        INTEGER NOT NULL," +
        "O_ORDERSTATUS    CHAR(1) NOT NULL," +
        "O_TOTALPRICE     DECIMAL(15,2) NOT NULL," +
        "O_ORDERDATE      DATE NOT NULL," +
        "O_ORDERPRIORITY  CHAR(15) NOT NULL," +
        "O_CLERK          CHAR(15) NOT NULL," +
        "O_SHIPPRIORITY   INTEGER NOT NULL," +
        "O_COMMENT        VARCHAR(79) NOT NULL) " +
        "USING COLUMN OPTIONS (PARTITION_BY 'O_ORDERKEY', " +
        "COLOCATE_WITH 'CUSTOMER' )")
    snc.sql("INSERT INTO ORDERS VALUES (1, 20000, 'O', 100.50, '2016-04-04', 'LOW', 'Clerk#001', 3, '')")
    snc.sql("INSERT INTO ORDERS VALUES (2, 20000, 'F', 1000, '2016-04-04', 'HIGH', 'Clerk#002', 1, '')")
    snc.sql("INSERT INTO ORDERS VALUES (3, 30000, 'F', 400, '2016-04-04', 'MEDIUM', 'Clerk#003', 2, '')")
    snc.sql("INSERT INTO ORDERS VALUES (4, 30000, 'O', 500, '2016-04-04', 'LOW', 'Clerk#002', 3, '')")

    pw.println("Selecting orders for all customers")
    val result = snc.sql("SELECT C_CUSTKEY, C_NAME, O_ORDERKEY, O_ORDERSTATUS, O_ORDERDATE, " +
        "O_TOTALPRICE FROM CUSTOMER, ORDERS WHERE C_CUSTKEY = O_CUSTKEY").collect()
    pw.println("CUSTKEY, NAME, ORDERKEY, ORDERSTATUS, ORDERDATE, ORDERDATE")
    pw.println("____________________________________________________________")
    result.foreach(pw.println)

    pw.println("****Done****")
  }

  def main(args: Array[String]): Unit = {
    // reducing the log level to minimize the messages on console
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    println("Creating a SnappySession")
    val spark: SparkSession = SparkSession
        .builder
        .appName("CollocatedJoinExample")
        .master("local[*]")
        .getOrCreate

    val snSession = new SnappySession(spark.sparkContext, existingSharedState = None)

    val pw = new PrintWriter(System.out, true)
    runCollocatedJoinQuery(snSession.snappyContext, pw)
    pw.close()
  }

}
