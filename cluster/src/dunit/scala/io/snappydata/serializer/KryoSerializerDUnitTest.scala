package io.snappydata.serializer

import io.snappydata.cluster.ClusterManagerTestBase

/**
 * Created by sachin on 14/6/16.
 * This class has tests that are used to verify basic functionality is working when kryoserializer is set
 */
class KryoSerializerDUnitTest(s: String) extends ClusterManagerTestBase(s) {

  bootProps.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  //Setting kryo registrator explicitly as SparkContext is created in ClusterManagerTest instead of lead node
  bootProps.setProperty("spark.kryo.registrator", "io.snappydata.serializer.SnappyKryoRegistrator")

  def createTestTable(): Unit = {

    val snc = org.apache.spark.sql.SnappyContext(sc)
    snc.sql(s"CREATE TABLE TEST(col1 int,col2 int) USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'Col1'," +
        "BUCKETS '1')")

  }

  def dropTestTable(): Unit = {
    val snc = org.apache.spark.sql.SnappyContext(sc)
    snc.sql(s"DROP table TEST")
  }

  def testCreateColumnTable(): Unit = {
    createTestTable()
    dropTestTable()

  }


  def testInsertColumnTable(): Unit = {
    createTestTable()
    val snc = org.apache.spark.sql.SnappyContext(sc)
    snc.sql("insert into test values(1,2)")
    snc.sql("insert into test values(2,3)")
    val count=snc.sql("select * from test").count()
    assert((count==2))
    dropTestTable()

    }

  def testRepartition(): Unit = {
    createTestTable()
    val snc = org.apache.spark.sql.SnappyContext(sc)
    snc.sql("insert into test values(1,2)")
    snc.sql("insert into test values(5,6)")
    snc.sql("select * from test").repartition(2)
    dropTestTable()

  }
}
