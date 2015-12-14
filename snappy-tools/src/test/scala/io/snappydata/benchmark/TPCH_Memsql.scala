package io.snappydata.benchmark

import java.sql.DriverManager

object TPCH_Memsql {
  def main(args: Array[String]) {

    val host = "127.0.0.1"
    val port = 3306
    val dbName = "ENERGY"
    val user = "root"
    val password = ""
    val outputTableName = "REGION"

    Class.forName("com.mysql.jdbc.Driver")
    val dbAddress = "jdbc:mysql://" + host + ":" + port + "/"
    val conn = DriverManager.getConnection(dbAddress, user, password)
    val stmt = conn.createStatement

    stmt.execute("DROP DATABASE IF EXISTS " + dbName)
    stmt.execute("CREATE DATABASE IF NOT EXISTS " + dbName)
    stmt.execute("USE " + dbName)

    TPCHReplicatedTable.createRegionTable_Memsql(stmt)
//    val filename="/home/kishor/snappy/TPCH_Data/GB1/region.tbl"
//    stmt.execute(s"load data infile \'$filename\' into table $outputTableName COLUMNS TERMINATED BY \'|\' LINES TERMINATED BY \'|\n\'");
//    println("Table is loaded")

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