package io.snappydata.benchmark.cluster

import java.sql.DriverManager

import io.snappydata.benchmark.TPCH_Memsql

/**
 * Created by kishor on 9/12/15.
 */
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

    for(i <- 1 to 3) {
      println("**************Iteration $i ***************************")
      TPCH_Memsql.execute("q1", isResultCollection, stmt)
      //TPCH_Memsql.execute("q2", isResultCollection, stmt)
      TPCH_Memsql.execute("q3", isResultCollection, stmt)
      TPCH_Memsql.execute("q4", isResultCollection, stmt)
      TPCH_Memsql.execute("q5", isResultCollection, stmt)
      TPCH_Memsql.execute("q6", isResultCollection, stmt)
      TPCH_Memsql.execute("q7", isResultCollection, stmt)
      TPCH_Memsql.execute("q8", isResultCollection, stmt)
      TPCH_Memsql.execute("q9", isResultCollection, stmt)
      TPCH_Memsql.execute("q10", isResultCollection, stmt)
      TPCH_Memsql.execute("q11", isResultCollection, stmt)
      TPCH_Memsql.execute("q12", isResultCollection, stmt)
      TPCH_Memsql.execute("q13", isResultCollection, stmt)
      TPCH_Memsql.execute("q14", isResultCollection, stmt)
      TPCH_Memsql.execute("q15", isResultCollection, stmt)
      TPCH_Memsql.execute("q16", isResultCollection, stmt)
      TPCH_Memsql.execute("q17", isResultCollection, stmt)
      TPCH_Memsql.execute("q18", isResultCollection, stmt)
      //TPCH_Memsql.execute("q19", isResultCollection, stmt)
      TPCH_Memsql.execute("q20", isResultCollection, stmt)
      //TPCH_Memsql.execute("q21", isResultCollection, stmt)
      TPCH_Memsql.execute("q22", isResultCollection, stmt)
      println("---------------------------------------------------------------------------------")
    }



    stmt.close();

  }
}
