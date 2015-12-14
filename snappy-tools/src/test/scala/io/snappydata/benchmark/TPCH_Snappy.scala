package io.snappydata.benchmark


import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by kishor on 19/10/15.
 */
object TPCH_Snappy {

  def main (args: Array[String]){
    val conf = new SparkConf().setAppName("TPCH_Snappy").setMaster("local[1]")

    val usingOptionString = """
          USING jdbc
          OPTIONS (
              URL 'jdbc:gemfirexd:;mcast-port=45672;persist-dd=false;',
              Driver 'com.pivotal.gemfirexd.jdbc.EmbeddedDriver'
          )"""

    val props = Map(
      "url" -> "jdbc:gemfirexd:;mcast-port=45672;persist-dd=false;user=app;password=app",
      "driver" -> "com.pivotal.gemfirexd.jdbc.EmbeddedDriver",
      "poolImpl" -> "tomcat",
      "poolProperties" -> "maxActive=300",
      "user" -> "app",
      "password" -> "app"
    )
    val sc = new SparkContext(conf)

    TPCHReplicatedTable.createRegionTable(usingOptionString, sc)
    TPCHReplicatedTable.populateRegionTable(props, sc)

    TPCHReplicatedTable.createNationTable(usingOptionString, sc)
    TPCHReplicatedTable.populateNationTable(props, sc)

    TPCHReplicatedTable.createSupplierTable(usingOptionString, sc)
    TPCHReplicatedTable.populateSupplierTable(props, sc)

    TPCHRowPartitionedTable.createPartTable(usingOptionString, sc)
    TPCHRowPartitionedTable.populatePartTable(props, sc)

    TPCHRowPartitionedTable.createPartSuppTable(usingOptionString, sc)
    TPCHRowPartitionedTable.populatePartSuppTable(props, sc)

    TPCHRowPartitionedTable.createCustomerTable(usingOptionString, sc)
    TPCHRowPartitionedTable.populateCustomerTable(props, sc)

    TPCHColumnPartitionedTable.createAndPopulateOrderTable(props, sc)
    TPCHColumnPartitionedTable.createAndPopulateLineItemTable(props, sc)

 //   TPCH_Snappy_Query.execute("Q", sc) //RUN SUCCESSFULLY

    TPCH_Snappy_Query.execute("Q1", sc) //RUN SUCCESSFULLY
    TPCH_Snappy_Query.execute("Q2", sc) //EXCEPTION DUE TO SELECT IN WHERE CLAUSE
    TPCH_Snappy_Query.execute("Q3", sc) //RUN BUT DOES NOT RETURN ANY RESULT
    TPCH_Snappy_Query.execute("Q4", sc) //EXCEPTION DUE TO SELECT IN WHERE CLAUSE
    TPCH_Snappy_Query.execute("Q5", sc) //RUN BUT DOES NOT RETURN ANY RESULT
    TPCH_Snappy_Query.execute("Q6", sc) //RUN SUCCESSFULLY
    TPCH_Snappy_Query.execute("Q7", sc) //RUN BUT DOES NOT RETURN ANY RESULT
    TPCH_Snappy_Query.execute("Q8", sc) //RUN BUT DOES NOT RETURN ANY RESULT
    TPCH_Snappy_Query.execute("Q9", sc) //RUN SUCCESSFULLY
    TPCH_Snappy_Query.execute("Q10", sc) //RUN SUCCESSFULLY
    TPCH_Snappy_Query.execute("Q11", sc) //EXCEPTION DUE TO SELECT IN WHERE CLAUSE WITH sum
    TPCH_Snappy_Query.execute("Q12", sc) //RUN SUCCESSFULLY
    TPCH_Snappy_Query.execute("Q13", sc)  //EXCEPTION DUE TO EXPRESSION  o_comment not like ‘%special%requests%
    TPCH_Snappy_Query.execute("Q14", sc) //RUN SUCCESSFULLY
    //TPCH_Snappy_Query.execute("Q15", sc)
    TPCH_Snappy_Query.execute("Q16", sc) //EXCEPTION DUE TO SELECT IN WHERE CLAUSE WITH sum
    TPCH_Snappy_Query.execute("Q17", sc) //EXCEPTION DUE TO SELECT IN WHERE CLAUSE WITH sum
    TPCH_Snappy_Query.execute("Q18", sc) //EXCEPTION DUE TO SELECT IN WHERE CLAUSE WITH sum
    TPCH_Snappy_Query.execute("Q19", sc) //MUST BE EASY TO FIX
    TPCH_Snappy_Query.execute("Q20", sc)
    TPCH_Snappy_Query.execute("Q21", sc)
    TPCH_Snappy_Query.execute("Q22", sc)

    sc.stop()

}
}


