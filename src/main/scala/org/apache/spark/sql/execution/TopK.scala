package org.apache.spark.sql.execution

import java.io.Externalizable
import java.io.ObjectOutput
import java.io.ObjectInput
import org.apache.spark.sql.execution.streamsummary.StreamSummaryAggregation

trait TopK extends Serializable{  
  def isStreamSummary: Boolean
}

private class TopKStub(var ss: Boolean) extends TopK with Externalizable{
  
  override def isStreamSummary: Boolean = this.ss
  
  //override def writeReplace(): Any= this
  
  override def writeExternal(out: ObjectOutput) = {
    out.writeBoolean(this.isStreamSummary)
  }

  
  override def readExternal( in: ObjectInput) = {
    this.ss = in.readBoolean    
  }
}