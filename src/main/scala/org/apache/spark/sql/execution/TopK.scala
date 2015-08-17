package org.apache.spark.sql.execution

import java.io.Externalizable
import java.io.ObjectOutput
import java.io.ObjectInput
import org.apache.spark.sql.execution.streamsummary.StreamSummaryAggregation

trait TopK extends Serializable{
  def getPartitionID: Int
  def getName: String
  def isStreamSummary: Boolean
  
  def writeReplace(): Any= new TopKStub(this.getName, this.getPartitionID, this.isStreamSummary)      
 
 def readResolve(): Any = {
   if(this.isStreamSummary) {
     StreamSummaryAggregation.get(this.getName, this.getPartitionID)
   }else {
     TopKHokusai.get(this.getName, this.getPartitionID)
   }
 }
 
  
}

private class TopKStub(var name: String, var partitionID: Int, var ss: Boolean) extends TopK with Externalizable{
  
  def this() = this(null, -1, false)
  override def getPartitionID: Int = this.partitionID
  override def getName: String = this.name
  override def isStreamSummary: Boolean = this.ss
  
  override def writeReplace(): Any= this
  
  override def writeExternal(out: ObjectOutput) = {
    out.writeUTF(this.getName)
    out.writeInt(this.getPartitionID)
    out.writeBoolean(this.isStreamSummary)
  }

  
  override def readExternal( in: ObjectInput) = {
    this.name = in.readUTF
    this.partitionID = in.readInt
    this.ss = in.readBoolean
    
  }
}