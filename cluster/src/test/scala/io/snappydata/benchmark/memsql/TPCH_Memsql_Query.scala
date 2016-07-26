package io.snappydata.benchmark.memsql

import java.sql.DriverManager

/**
  * Created by kishor on 9/12/15.
  */
object TPCH_Memsql_Query {

   def main(args: Array[String]) {

     val host = args(0)
     val queries:Array[String] = args(1).split(",")
     val port = 3307
     val dbName = "TPCH"
     val user = "root"
     val password = ""

     Class.forName("com.mysql.jdbc.Driver")
     val dbAddress = "jdbc:mysql://" + host + ":" + port + "/"
     val conn = DriverManager.getConnection(dbAddress, user, password)
     val stmt = conn.createStatement

     val isResultCollection =false

     stmt.execute("USE " + dbName)

     for(query <- queries)
       query match {
         case "1" =>   TPCH_Memsql.execute("q1", isResultCollection, stmt)
         case "2" =>   TPCH_Memsql.execute("q2", isResultCollection, stmt)//taking hours to execute in Snappy
         case "3"=>   TPCH_Memsql.execute("q3", isResultCollection, stmt)
         case "4" =>   TPCH_Memsql.execute("q4", isResultCollection, stmt)
         case "5" =>   TPCH_Memsql.execute("q5", isResultCollection, stmt)
         case "6" =>   TPCH_Memsql.execute("q6", isResultCollection, stmt)
         case "7" =>   TPCH_Memsql.execute("q7", isResultCollection, stmt)
         case "8" =>   TPCH_Memsql.execute("q8", isResultCollection, stmt)
         case "9" =>   TPCH_Memsql.execute("q9", isResultCollection, stmt) //taking hours to execute in Snappy
         case "10" =>   TPCH_Memsql.execute("q10", isResultCollection, stmt)
         case "11" =>   TPCH_Memsql.execute("q11", isResultCollection, stmt)
         case "12" =>   TPCH_Memsql.execute("q12", isResultCollection, stmt)
         case "13" =>   TPCH_Memsql.execute("q13", isResultCollection, stmt)
         case "14" =>   TPCH_Memsql.execute("q14", isResultCollection, stmt)
         case "15" =>   TPCH_Memsql.execute("q15", isResultCollection, stmt)
         case "16" =>   TPCH_Memsql.execute("q16", isResultCollection, stmt)
         case "17" =>   TPCH_Memsql.execute("q17", isResultCollection, stmt)
         case "18" =>   TPCH_Memsql.execute("q18", isResultCollection, stmt)
         case "19" =>   TPCH_Memsql.execute("q19", isResultCollection, stmt) //not working in local mode hence not executing it for cluster mode too
         case "20" =>   TPCH_Memsql.execute("q20", isResultCollection, stmt)
         case "21" =>   TPCH_Memsql.execute("q21", isResultCollection, stmt) //not working in local mode hence not executing it for cluster mode too
         case "22" =>   TPCH_Memsql.execute("q22", isResultCollection, stmt)
           println("---------------------------------------------------------------------------------")
       }

//     TPCH_Memsql.execute("q1", isResultCollection, stmt)
//     //TPCH_Memsql.execute("q2", isResultCollection, stmt)
//     TPCH_Memsql.execute("q3", isResultCollection, stmt)
//     TPCH_Memsql.execute("q4", isResultCollection, stmt)
//     TPCH_Memsql.execute("q5", isResultCollection, stmt)
//     TPCH_Memsql.execute("q6", isResultCollection, stmt)
//     TPCH_Memsql.execute("q7", isResultCollection, stmt)
//     TPCH_Memsql.execute("q8", isResultCollection, stmt)
//     //TPCH_Memsql.execute("q9", isResultCollection, stmt)taking hours to execute in snappy
//     TPCH_Memsql.execute("q10", isResultCollection, stmt)
//     TPCH_Memsql.execute("q11", isResultCollection, stmt)
//     TPCH_Memsql.execute("q12", isResultCollection, stmt)
//     TPCH_Memsql.execute("q13", isResultCollection, stmt)
//     TPCH_Memsql.execute("q14", isResultCollection, stmt)
//     TPCH_Memsql.execute("q15", isResultCollection, stmt)
//     TPCH_Memsql.execute("q16", isResultCollection, stmt)
//     TPCH_Memsql.execute("q17", isResultCollection, stmt)
//     TPCH_Memsql.execute("q18", isResultCollection, stmt)
//    //TPCH_Memsql.execute("q19", isResultCollection, stmt)
//     TPCH_Memsql.execute("q20", isResultCollection, stmt)
//     //TPCH_Memsql.execute("q21", isResultCollection, stmt)
//     TPCH_Memsql.execute("q22", isResultCollection, stmt)



     stmt.close();
     TPCH_Memsql.close()

   }
 }
