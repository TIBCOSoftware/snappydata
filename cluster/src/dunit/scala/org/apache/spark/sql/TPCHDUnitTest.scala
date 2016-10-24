package org.apache.spark.sql

import java.io.{File, FileOutputStream, PrintStream}

import io.snappydata.benchmark.snappy.TPCH_Snappy
import io.snappydata.benchmark.{TPCHColumnPartitionedTable, TPCHReplicatedTable}
import io.snappydata.cluster.ClusterManagerTestBase

import org.apache.spark.SparkContext


/**
 * Created by kishor on 14/9/16.
 */
class TPCHDUnitTest(s: String) extends ClusterManagerTestBase(s){

  val queries= Array("q1","q2","q3","q4","q5","q6","q7","q8","q9","q10","q11","q12","q13","q14","q15","q16","q17","q18","q19","q20","q21","q22")

  def testSnappy(): Unit = {
    val snc = SnappyContext(sc)
    createAndLoadTables(snc, true)
    queryExecution(snc, true)
    validateResult(snc, true)
  }

  def _testSpark(): Unit = {
    val snc = SnappyContext(sc)
    createAndLoadTables(snc, false)
    queryExecution(snc, false)
    validateResult(snc, false)
  }


  private def createAndLoadTables(snc: SnappyContext, isSnappy:Boolean): Unit = {
    val tpchDataPath = getClass.getResource("/TPCH").getPath

    val usingOptionString = s"""
           USING row
           OPTIONS ()"""
    
    TPCHReplicatedTable.createPopulateRegionTable(usingOptionString, snc, tpchDataPath, isSnappy, null)
    TPCHReplicatedTable.createPopulateNationTable(usingOptionString, snc, tpchDataPath, isSnappy, null)
    TPCHReplicatedTable.createPopulateSupplierTable(usingOptionString, snc, tpchDataPath, isSnappy, null)

    val buckets_Order_Lineitem = "5"
    val buckets_Cust_Part_PartSupp = "5"
    TPCHColumnPartitionedTable.createAndPopulateOrderTable(snc, tpchDataPath, isSnappy, buckets_Order_Lineitem, null)
    TPCHColumnPartitionedTable.createAndPopulateLineItemTable(snc, tpchDataPath, isSnappy, buckets_Order_Lineitem, null)
    TPCHColumnPartitionedTable.createPopulateCustomerTable(snc, tpchDataPath, isSnappy, buckets_Cust_Part_PartSupp, null)
    TPCHColumnPartitionedTable.createPopulatePartTable(snc, tpchDataPath, isSnappy, buckets_Cust_Part_PartSupp, null)
    TPCHColumnPartitionedTable.createPopulatePartSuppTable(snc, tpchDataPath, isSnappy, buckets_Cust_Part_PartSupp, null)
  }

  private def queryExecution(snc: SnappyContext, isSnappy:Boolean): Unit = {
    snc.sql(s"set spark.sql.shuffle.partitions= 4")
    snc.sql(s"set spark.sql.crossJoin.enabled = true")

    queries.foreach(query => TPCH_Snappy.execute(query, snc, true, isSnappy))
  }

  private def validateResult(snc: SnappyContext, isSnappy:Boolean): Unit = {
    val sc:SparkContext = snc.sparkContext

    val fineName = if(isSnappy)"Result_Snappy.out" else "Result_Spark.out"

    var resultFileStream: FileOutputStream = new FileOutputStream(new File(fineName))
    var resultOutputStream:PrintStream = new PrintStream(resultFileStream)

    for(query <- queries){
      println(s"For Query $query")

      val expectedFile = sc.textFile(getClass.getResource(s"/TPCH/RESULT/Snappy_${query}.out").getPath)

      val queryFileName = if(isSnappy) s"Snappy_${query}.out" else s"Spark_${query}.out"
      val actualFile = sc.textFile(queryFileName)


      val expectedLineSet = expectedFile.collect().toList.sorted
      val actualLineSet = actualFile.collect().toList.sorted

      if(!actualLineSet.equals(expectedLineSet)) {
        if (!(expectedLineSet.size == actualLineSet.size)) {
          resultOutputStream.println(s"For $query result count mismatched observed")
        } else {
          for ((expectedLine, actualLine) <- (expectedLineSet zip actualLineSet)) {
            if (!expectedLine.equals(actualLine)) {
              resultOutputStream.println(s"For $query result mismatched observed")
              resultOutputStream.println(s"Excpected : $expectedLine")
              resultOutputStream.println(s"Found     : $actualLine")
              resultOutputStream.println(s"-------------------------------------")
            }
          }
        }
      }
    }
    resultOutputStream.close()
    resultFileStream.close()

    val resultOutputFile = sc.textFile(fineName)
    assert(resultOutputFile.count() == 0, s"Query mismatch Observed. Look at Result_Snappy.out for detailed failure")
  }

}
