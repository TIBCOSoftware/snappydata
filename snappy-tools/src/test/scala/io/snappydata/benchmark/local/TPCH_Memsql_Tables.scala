package io.snappydata.benchmark.local

import java.sql.DriverManager

import io.snappydata.benchmark.{TPCHColumnPartitionedTable, TPCHRowPartitionedTable, TPCHReplicatedTable}

object TPCH_Memsql_Tables {

  def main(args: Array[String]) {

    val host = "127.0.0.1"
    val port = 3306
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

    TPCHRowPartitionedTable.createPartTable_Memsql(stmt)

    TPCHRowPartitionedTable.createPartSuppTable_Memsql(stmt)

    TPCHRowPartitionedTable.createCustomerTable_Memsql(stmt)

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