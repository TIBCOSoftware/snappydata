package org.apache.spark.sql.execution

trait TopK {
  def getPartitionID: Int
}

private class TopKStub(partitionID: Int) extends TopK {
  override def getPartitionID: Int = this.partitionID
}