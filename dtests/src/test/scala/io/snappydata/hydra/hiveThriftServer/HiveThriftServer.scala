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
package io.snappydata.hydra.hiveThriftServer

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import com.typesafe.config.Config
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

class HiveThriftServer extends SnappySQLJob {
  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()

  override def runSnappyJob(sc: SnappySession, jobConfig: Config): Any = {

    // scalastyle:off println
    println("Starting the Hive Thrift Server testing job.....")
    val connection : Connection = DriverManager.getConnection(
      "jdbc:hive2://localhost:10000", "app", "app")
    val stmt : Statement = connection.createStatement()
    val rs : ResultSet = stmt.executeQuery("show databases")
    var result : String = ""
    while(rs.next) {
      result = result + rs.getString(1) + "\n"
      result = result + rs.getString(2) + "\n"
      result = result + rs.getString(3)
    }

    println("show databases : " + result)


    println("Finished the Hive Thrift Server testing.....")

  }
}
