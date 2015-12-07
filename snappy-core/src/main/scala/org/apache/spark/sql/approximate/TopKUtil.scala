package org.apache.spark.sql.approximate

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark.sql.{SnappyContext, Row}
import org.apache.spark.sql.execution.streamsummary.StreamSummaryAggregation
import org.apache.spark.sql.hive.QualifiedTableName
import org.apache.spark.{Partitioner, Partition, TaskContext, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.{TopKStub, KeyFrequencyWithTimestamp, TopKWrapper, TopKHokusai, TopK}

import org.apache.spark.sql.snappy._

/**
 * Created by rishim on 3/12/15.
 */
object TopKUtil {

  def createTopKRDD(name: String, context: SparkContext,
      isStreamSummary: Boolean): RDD[(Int, TopK)] = {
    val partCount = Utils.getAllExecutorsMemoryStatus(context).
        keySet.size * Runtime.getRuntime.availableProcessors() * 2
    Utils.getFixedPartitionRDD[(Int, TopK)](context,
      (tc: TaskContext, part: Partition) => {
        scala.collection.Iterator(part.index -> TopKHokusai.createDummy)
      }, new Partitioner() {
        override def numPartitions: Int = partCount

        override def getPartition(key: Any) =
          scala.math.abs(key.hashCode()) % partCount
      }, partCount)
  }

  private def getEpoch0AndIterator[T: ClassTag](name: String, topkWrapper: TopKWrapper,
      iterator: Iterator[Any]): (() => Long, Iterator[Any], Int) = {
    if (iterator.hasNext) {
      var tupleIterator = iterator
      val tsCol = if (topkWrapper.timeInterval > 0) {
        topkWrapper.timeSeriesColumn
      }
      else {
        -1
      }
      val epoch = () => {
        if (topkWrapper.epoch != -1L) {
          topkWrapper.epoch
        } else if (tsCol >= 0) {
          var epoch0 = -1L
          val iter = tupleIterator.asInstanceOf[Iterator[(T, Any)]]
          val tupleBuf = new mutable.ArrayBuffer[(T, Any)](4)

          // assume first row will have the least time
          // TODO: this assumption may not be correct and we may need to
          // do something more comprehensive
          do {
            val tuple = iter.next()
            epoch0 = tuple match {
              case (_, (_, epochh: Long)) => epochh
              case (_, epochh: Long) => epochh
            }

            tupleBuf += tuple.copy()
          } while (epoch0 <= 0)
          tupleIterator = tupleBuf.iterator ++ iter
          epoch0
        } else {
          System.currentTimeMillis()
        }

      }
      (epoch, tupleIterator, tsCol)
    } else {
      null
    }
  }

  private def addDataForTopK[T: ClassTag](topKWrapper: TopKWrapper,
      tupleIterator: Iterator[Any], topK: TopK, tsCol: Int, time: Long): Unit = {

    val stsummary = topKWrapper.stsummary
    val streamSummaryAggr: StreamSummaryAggregation[T] = if (stsummary) {
      topK.asInstanceOf[StreamSummaryAggregation[T]]
    } else {
      null
    }
    val topKHokusai = if (!stsummary) {
      topK.asInstanceOf[TopKHokusai[T]]
    } else {
      null
    }
    // val topKKeyIndex = topKWrapper.schema.fieldIndex(topKWrapper.key.name)
    if (tsCol < 0) {
      if (stsummary) {
        throw new IllegalStateException(
          "Timestamp column is required for stream summary")
      }
      topKWrapper.frequencyCol match {
        case None =>
          topKHokusai.addEpochData(tupleIterator.asInstanceOf[Iterator[T]].
              toSeq.foldLeft(
            scala.collection.mutable.Map.empty[T, Long]) {
            (m, x) => m + ((x, m.getOrElse(x, 0L) + 1))
          }, time)
        case Some(freqCol) =>
          val datamap = mutable.Map[T, Long]()
          tupleIterator.asInstanceOf[Iterator[(T, Long)]] foreach {
            case (key, freq) =>
              datamap.get(key) match {
                case Some(prevvalue) => datamap +=
                    (key -> (prevvalue + freq))
                case None => datamap +=
                    (key -> freq)
              }

          }
          topKHokusai.addEpochData(datamap, time)
      }
    } else {
      val dataBuffer = new mutable.ArrayBuffer[KeyFrequencyWithTimestamp[T]]
      val buffer = topKWrapper.frequencyCol match {
        case None =>
          tupleIterator.asInstanceOf[Iterator[(T, Long)]] foreach {
            case (key, timeVal) =>
              dataBuffer += new KeyFrequencyWithTimestamp[T](key, 1L, timeVal)
          }
          dataBuffer
        case Some(freqCol) =>
          tupleIterator.asInstanceOf[Iterator[(T, (Long, Long))]] foreach {
            case (key, (freq, timeVal)) =>
              dataBuffer += new KeyFrequencyWithTimestamp[T](key,
                freq, timeVal)
          }
          dataBuffer
      }
      if (stsummary) {
        streamSummaryAggr.addItems(buffer)
      }
      else {
        topKHokusai.addTimestampedData(buffer)
      }
    }
  }

  def populateTopK[T: ClassTag](rows: RDD[Row], topkWrapper: TopKWrapper,
      context: SnappyContext, name: QualifiedTableName, topKRDD: RDD[(Int, TopK)],
      time: Long) {
    val partitioner = topKRDD.partitioner.get
    // val pairRDD = rows.map[(Int, Any)](topkWrapper.rowToTupleConverter(_, partitioner))
    val batches = mutable.ArrayBuffer.empty[(Int, mutable.ArrayBuffer[Any])]
    val pairRDD = rows.mapPartitionsPreserve[(Int, mutable.ArrayBuffer[Any])](iter => {
      val map = iter.foldLeft(mutable.Map.empty[Int, mutable.ArrayBuffer[Any]])((m, x) => {
        val (partitionID, elem) = topkWrapper.rowToTupleConverter(x, partitioner)
        val list = m.getOrElse(partitionID, mutable.ArrayBuffer[Any]()) += elem
        if (list.size > 1000) {
          batches += partitionID -> list
          m -= partitionID
        } else {
          m += (partitionID -> list)
        }
        m
      })
      map.toIterator ++ batches.iterator
    }, preservesPartitioning = true)

    val nameAsString = name.toString
    val newTopKRDD = topKRDD.cogroup(pairRDD).mapPartitionsPreserve[(Int, TopK)](
      iterator => {
        val (key, (topkIterable, dataIterable)) = iterator.next()
        val tsCol = if (topkWrapper.timeInterval > 0) {
          topkWrapper.timeSeriesColumn
        }
        else {
          -1
        }

        topkIterable.head match {
          case z: TopKStub =>
            val totalDataIterable = dataIterable.foldLeft[mutable.ArrayBuffer[
                Any]](mutable.ArrayBuffer.empty[Any])((m, x) => {
              if (m.size > x.size) {
                m ++= x
              } else {
                x ++= m
              }
            })
            val (epoch0, iter, tsCol) = getEpoch0AndIterator[T](nameAsString,
              topkWrapper, totalDataIterable.iterator)
            val topK = if (topkWrapper.stsummary) {
              StreamSummaryAggregation.create[T](topkWrapper.size,
                topkWrapper.timeInterval, epoch0, topkWrapper.maxinterval)
            } else {
              TopKHokusai.create[T](topkWrapper.cms, topkWrapper.size,
                tsCol, topkWrapper.timeInterval, epoch0)
            }
            addDataForTopK[T](topkWrapper,
              iter, topK, tsCol, time)
            scala.collection.Iterator(key -> topK)

          case topK: TopK =>
            dataIterable.foreach { x => addDataForTopK[T](
              topkWrapper, x.iterator, topK, tsCol, time)
            }

            scala.collection.Iterator(key -> topK)
        }

      }, preservesPartitioning = true)

    newTopKRDD.persist()
    // To allow execution of RDD
    newTopKRDD.count()
    context.catalog.topKStructures.put(name, topkWrapper -> newTopKRDD)
    // Unpersist old rdd in a write lock

    context.topKLocks(name.toString()).executeInWriteLock {
      topKRDD.unpersist(false)
    }
  }
}
