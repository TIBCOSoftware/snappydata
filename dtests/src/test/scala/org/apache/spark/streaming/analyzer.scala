package org.apache.spark.streaming

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.snappy._
import org.apache.spark.streaming.{Consts, SnappyStreamingContext}
import java.sql.Timestamp
import org.apache.log4j.Logger
import org.apache.log4j.Level

class analyzer() extends java.io.Serializable {

  def analyze(snsc: SnappyStreamingContext) {
  
    def acc = Consts.SRC1_STREAMTABLE_NAME
    def bio = Consts.SRC2_STREAMTABLE_NAME
    def env = Consts.SRC3_STREAMTABLE_NAME

    // The test below is OK.
    // But snsc.getSchemaDStream("STREAM_ACCELERATION") does not work.
    // I got error "STREAM_ACCELERATION is not STREAM TABLE"
    // Why??

    val sds1FromTable = snsc.registerCQ(
      s"select * from ${acc}"
    )
    sds1FromTable.foreachDataFrame { df =>
      df.show
    }
    val sds2FromTable = snsc.registerCQ(
      s"select * from ${bio}"
    )
    sds2FromTable.foreachDataFrame { df =>
      df.show
    }
    val sds3FromTable = snsc.registerCQ(
      s"select * from ${env}"
    )
    sds3FromTable.foreachDataFrame { df =>
      df.show
    }
  }

}
