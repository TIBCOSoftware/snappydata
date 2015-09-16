package org.apache.spark.sql.execution

import java.io.Serializable
import java.io.ObjectOutput
import java.io.ObjectInput
import org.apache.spark.sql.execution.streamsummary.StreamSummaryAggregation

trait TopK extends Serializable

class TopKStub extends TopK with Serializable