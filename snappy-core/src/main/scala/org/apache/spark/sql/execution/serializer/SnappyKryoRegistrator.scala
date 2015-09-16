package org.apache.spark.sql.execution.serializer

import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.sql.execution.TopKHokusai
import org.apache.spark.sql.execution.CMSParams
import org.apache.spark.sql.execution.streamsummary.StreamSummaryAggregation

class SnappyKryoRegistrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[TopKHokusai[_]], new TopkHokusaiKryoSerializer())
    kryo.register(classOf[StreamSummaryAggregation[_]], new StreamSummaryAggregationKryoSerializer())
  }
}