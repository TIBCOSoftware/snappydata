package org.apache.spark.sql.execution.serializer

import org.apache.spark.sql.execution.streamsummary.StreamSummaryAggregation
import org.apache.spark.sql.execution.TopKHokusai
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.io.Input

class TopkHokusaiKryoSerializer extends Serializer[TopKHokusai[_]] {

  override def write(kryo: Kryo, output: Output, obj: TopKHokusai[_]) {
    TopKHokusai.write(kryo, output, obj)
  }

  override def read(kryo: Kryo, input: Input, typee: Class[TopKHokusai[_]]): TopKHokusai[_] = {
    TopKHokusai.read(kryo, input)
  }
}

class StreamSummaryAggregationKryoSerializer extends Serializer[StreamSummaryAggregation[_]] {

  override def write(kryo: Kryo, output: Output, obj: StreamSummaryAggregation[_]) {
    StreamSummaryAggregation.write(kryo, output, obj)
  }

  override def read(kryo: Kryo, input: Input,
    typee: Class[StreamSummaryAggregation[_]]): StreamSummaryAggregation[_] = {
    StreamSummaryAggregation.read(kryo, input)
  }
}