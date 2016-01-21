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
package io.snappydata.benchmark.cluster

import java.sql.DriverManager

import io.snappydata.benchmark.TPCH_Memsql

object Cluster_TPCH_Memsql_Query {

  def main(args: Array[String]) {

    val host = "rdu-w27"
    val port = 3306
    val dbName = "TPCH"
    val user = "root"
    val password = ""

    Class.forName("com.mysql.jdbc.Driver")
    val dbAddress = "jdbc:mysql://" + host + ":" + port + "/"
    val conn = DriverManager.getConnection(dbAddress, user, password)
    val stmt = conn.createStatement

    val isResultCollection = args(0).toBoolean

    stmt.execute("USE " + dbName)

//    TPCH_Memsql.execute("q1", isResultCollection, stmt)
//    TPCH_Memsql.execute("q2", isResultCollection, stmt)
//    //TPCH_Memsql.execute("q3", isResultCollection, stmt)
//    TPCH_Memsql.execute("q4", isResultCollection, stmt)
//    //TPCH_Memsql.execute("q5", isResultCollection, stmt)
    TPCH_Memsql.execute("q6", isResultCollection, stmt)
    TPCH_Memsql.execute("q7", isResultCollection, stmt)
   // TPCH_Memsql.execute("q8", isResultCollection, stmt)
    //TPCH_Memsql.execute("q9", isResultCollection, stmt)
    TPCH_Memsql.execute("q10", isResultCollection, stmt)
    TPCH_Memsql.execute("q11", isResultCollection, stmt)
    TPCH_Memsql.execute("q12", isResultCollection, stmt)
    TPCH_Memsql.execute("q13", isResultCollection, stmt)
    TPCH_Memsql.execute("q14", isResultCollection, stmt)
    TPCH_Memsql.execute("q15", isResultCollection, stmt)
    TPCH_Memsql.execute("q16", isResultCollection, stmt)
    TPCH_Memsql.execute("q17", isResultCollection, stmt)
    //TPCH_Memsql.execute("q18", isResultCollection, stmt)
    //TPCH_Memsql.execute("q19", isResultCollection, stmt)
    TPCH_Memsql.execute("q20", isResultCollection, stmt)
    //TPCH_Memsql.execute("q21", isResultCollection, stmt)
    TPCH_Memsql.execute("q22", isResultCollection, stmt)



    stmt.close();

  }
}
