/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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
package io.snappydata.benchmark.memsql

import java.sql.DriverManager

import io.snappydata.benchmark.{TPCHColumnPartitionedTable, TPCHReplicatedTable}

object TPCH_Memsql_Tables {

   def main(args: Array[String]) {

     val host = args(0)
     val port = args(1)
     val dbName = "TPCH"
     val user = "root"
     val password = ""

     Class.forName("com.mysql.jdbc.Driver")
     val dbAddress = "jdbc:mysql://" + host + ":" + port + "/"
     val conn = DriverManager.getConnection(dbAddress, user, password)
     val stmt = conn.createStatement

     stmt.execute("DROP DATABASE IF EXISTS " + dbName)
     stmt.execute("CREATE DATABASE IF NOT EXISTS " + dbName)
     stmt.execute("USE " + dbName)

     TPCHReplicatedTable.createRegionTable_Memsql(stmt)

     TPCHReplicatedTable.createNationTable_Memsql(stmt)

     TPCHReplicatedTable.createSupplierTable_Memsql(stmt)

     TPCHColumnPartitionedTable.createPartTable_Memsql(stmt)

     TPCHColumnPartitionedTable.createPartSuppTable_Memsql(stmt)

     TPCHColumnPartitionedTable.createCustomerTable_Memsql(stmt)

     TPCHColumnPartitionedTable.createOrderTable_Memsql(stmt)

     TPCHColumnPartitionedTable.createLineItemTable_Memsql(stmt)

     var rs = stmt.executeQuery("SHOW TABLES")
     println("Tables" + rs)
     while (rs.next()) {
       System.out.println(rs.getString(1));
     }

     stmt.close();


   }
 }
