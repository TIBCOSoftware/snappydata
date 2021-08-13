/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package org.apache.spark.sql.execution.columnar

import scala.collection.AbstractIterator

import com.gemstone.gemfire.internal.cache.{ExternalTableMetaData, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.access.heap.MemHeapScanController
import com.pivotal.gemfirexd.internal.engine.store.AbstractCompactExecRow
import com.pivotal.gemfirexd.internal.iapi.store.access.ScanController
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.impl.ColumnFormatRelation
import org.apache.spark.sql.execution.row.RowTableScan
import org.apache.spark.sql.execution.{BufferedRowIterator, CodegenSupportOnExecutor, LeafExecNode, SnapshotConnectionListener, WholeStageCodegenExec}
import org.apache.spark.sql.store.CodeGeneration
import org.apache.spark.sql.types._
import org.apache.spark.{Logging, TaskContext}

final class ColumnBatchCreator(
    bufferRegion: PartitionedRegion,
    val tableName: String,
    val columnTableName: String,
    val schema: StructType,
    val externalStore: ExternalStore,
    val compressionCodec: String) extends Logging {

  def createAndStoreBatch(sc: ScanController, row: AbstractCompactExecRow, bucketID: Int,
      dependents: Seq[ExternalTableMetaData]): ObjectOpenHashSet[AnyRef] = {
    try {
      val memHeapScanController = sc.asInstanceOf[MemHeapScanController]
      memHeapScanController.setAddRegionAndKey()
      val keySet = new ObjectOpenHashSet[AnyRef]
      // noinspection TypeAnnotation
      val execRows = new AbstractIterator[AbstractCompactExecRow] {

        var hasNext: Boolean = memHeapScanController.next()

        override def next(): AbstractCompactExecRow = {
          if (hasNext) {
            memHeapScanController.fetch(row)
            keySet.add(row.getAllRegionAndKeyInfo.first().getKey)
            hasNext = memHeapScanController.next()
            row
          } else {
            throw new NoSuchElementException()
          }
        }
      }
      Utils.withTempTaskContextIfAbsent {
        val compileKey = CodeGeneration.createBatchKey(tableName)
        val gen = CodeGeneration.compileCode(compileKey, schema.fields, () => {
          val tableScan = RowTableScan(schema.toAttributes, schema,
            dataRDD = null, numBuckets = -1, partitionColumns = Nil,
            partitionColumnAliases = Nil, tableName, baseRelation = null, caseSensitive = true)
          // sending negative values for batch size and delta rows will create
          // only one column batch that will not be checked for size again
          val insertPlan = ColumnInsertExec(tableScan, Nil, Nil,
            numBuckets = -1, isPartitioned = true, None,
            (-bufferRegion.getColumnBatchSize, -1, compressionCodec), columnTableName,
            onExecutor = true, schema, externalStore, useMemberVariables = false)
          // now generate the code with the help of WholeStageCodegenExec
          // this is only used for local code generation while its RDD semantics
          // and related methods are all ignored
          val (ctx, code) = ExternalStoreUtils.codeGenOnExecutor(
            WholeStageCodegenExec(insertPlan), insertPlan)
          val references = ctx.references
          (code, references.toArray)
        })
        val iter = gen._1.generate(gen._2).asInstanceOf[BufferedRowIterator]
        iter.init(bucketID, Array(execRows.asInstanceOf[Iterator[InternalRow]]))
        while (iter.hasNext) {
          iter.next() // ignore result which is number of inserted rows
        }
        if (dependents.nonEmpty) {
          // expect both TaskContext and SnapshotConnectionListener to be present here
          val conn = SnapshotConnectionListener.getExisting(TaskContext.get()).get.connection
          val indexStatements = dependents.map(ColumnFormatRelation
              .getIndexUpdateStruct(_, conn))
          indexStatements.foreach(_._2.executeBatch())
        }
        keySet
      }
    } finally {
      sc.close()
    }
  }

  /**
   * This returns a [[ColumnBatchRowsBuffer]] that can be used for
   * insertion of rows as they appear. Currently used by sampler that
   * does not have any indexes so there is no dependents handling here.
   */
  def createColumnBatchBuffer(columnBatchSize: Int,
      columnMaxDeltaRows: Int): ColumnBatchRowsBuffer = {
    val compileKey = CodeGeneration.sampleInsertKey(tableName)
    val gen = CodeGeneration.compileCode(compileKey, schema.fields, () => {
      val bufferPlan = CallbackColumnInsert(schema)
      // no puts into row buffer for now since it causes split of rows held
      // together and thus failures in ClosedFormAccuracySuite etc

      // NOTE: Utils.withTempTaskContextIfAbsent is not required as the TaskContext is always
      // non-null here because the callers invoke this method in the body of RDD compute
      val insertPlan = ColumnInsertExec(bufferPlan, Nil, Nil,
        numBuckets = -1, isPartitioned = false, None, (columnBatchSize, -1, compressionCodec),
        columnTableName, onExecutor = true, schema, externalStore,
        useMemberVariables = true)
      // now generate the code with the help of WholeStageCodegenExec
      // this is only used for local code generation while its RDD semantics
      // and related methods are all ignored
      val (ctx, code) = ExternalStoreUtils.codeGenOnExecutor(
        WholeStageCodegenExec(insertPlan), insertPlan)
      val references = ctx.references.toArray
      (code, references)
    })
    val iter = gen._1.generate(gen._2).asInstanceOf[BufferedRowIterator]
    iter.init(0, Array.empty)
    // get the ColumnBatchRowsBuffer by reflection
    val rowsBufferMethod = iter.getClass.getMethod("getRowsBuffer")
    rowsBufferMethod.setAccessible(true)
    rowsBufferMethod.invoke(iter).asInstanceOf[ColumnBatchRowsBuffer]
  }
}

trait ColumnBatchRowsBuffer {

  def startRows(bucketId: Int): Unit

  def appendRow(row: InternalRow): Unit

  def endRows(): Unit
}

/**
 * This class is an adapter over the iterator model as provided by generated
 * code to closure callbacks model as required by StratifiedSampler.append
 */
case class CallbackColumnInsert(_schema: StructType)
    extends LeafExecNode with CodegenSupportOnExecutor {

  override def output: Seq[Attribute] = _schema.toAttributes

  override lazy val schema: StructType = _schema

  var bucketIdTerm: String = _
  var resetInsertions: String = _

  override def inputRDDs(): Seq[RDD[InternalRow]] =
    throw new UnsupportedOperationException("unexpected invocation")

  override protected def doProduce(ctx: CodegenContext): String = {
    val row = ctx.freshName("row")
    val hasResults = ctx.freshName("hasResults")
    val clearResults = ctx.freshName("clearResults")
    val rowsBuffer = ctx.freshName("rowsBuffer")
    val rowsBufferClass = classOf[ColumnBatchRowsBuffer].getName
    ctx.addMutableState(rowsBufferClass, rowsBuffer, "")
    // add bucketId variable set to -1 by default
    bucketIdTerm = ctx.freshName("bucketId")
    resetInsertions = ctx.freshName("resetInsertionsCount")
    ctx.addMutableState("int", bucketIdTerm, s"$bucketIdTerm = -1;")
    val columnsExpr = output.zipWithIndex.map { case (a, i) =>
      BoundReference(i, a.dataType, a.nullable)
    }
    ctx.INPUT_ROW = row
    ctx.currentVars = null
    val columnsInput = ctx.generateExpressions(columnsExpr)
    ctx.addNewFunction(hasResults,
      s"""
         |public final boolean $hasResults() {
         |  return !currentRows.isEmpty();
         |}
      """.stripMargin)
    ctx.addNewFunction(clearResults,
      s"""
         |public final void $clearResults() {
         |  currentRows.clear();
         |}
      """.stripMargin)
    ctx.addNewFunction("getRowsBuffer",
      s"""
         |public $rowsBufferClass getRowsBuffer() throws java.io.IOException {
         |  $clearResults(); // clear any old results
         |  $resetInsertions(); // reset the counters
         |  // initialize the $rowsBuffer
         |  if (this.$rowsBuffer == null) {
         |    processNext();
         |    $clearResults(); // clear the accumulated dummy zero result
         |  }
         |  return this.$rowsBuffer;
         |}
      """.stripMargin)
    // create the rows buffer implementation as an inner anonymous
    // class so that it can be fit easily in the iterator model of
    // doProduce/doConsume having access to all the final local variables
    // declared as part of the doProduce of the parent ColumnInsertExec;
    // an alternative could have been an inner class in the getRowsBuffer call
    // but the below allows capturing encoders as final member variables in
    // its closure thunk and so the encoder calls are more likely to be inlined
    s"""
       |if (this.$rowsBuffer == null) {
       |  this.$rowsBuffer = new $rowsBufferClass() {
       |    public void startRows(int bucketId) throws java.io.IOException {
       |      // set the bucketId
       |      $bucketIdTerm = bucketId;
       |      if ($hasResults()) {
       |        $clearResults(); // clear any old results
       |        // reset the size and re-initialize encoders
       |        $resetInsertions();
       |        processNext();
       |        $clearResults(); // clear the accumulated dummy zero result
       |      }
       |    }
       |
       |    public void appendRow(InternalRow $row) {
       |      // The code below will have access to the local variables
       |      // of the doProduce code of ColumnInsertExec so can be injected
       |      // as such. It also relies on the fact that ColumnInsertExec
       |      // local variables will be initialized and stored back into
       |      // member variables so that second call to processNext will
       |      // have access to the variables created before once this class
       |      // has been initialized explicitly by the first call.
       |      ${consume(ctx, columnsInput, row)}
       |    }
       |
       |    public void endRows() throws java.io.IOException {
       |      // invoke the parent's processNext() that will just insert
       |      // the column batch created so far
       |      processNext();
       |    }
       |  };
       |}
    """.stripMargin
  }

  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("unexpected invocation")
}
