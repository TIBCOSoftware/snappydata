/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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

import java.util.UUID

import scala.collection.AbstractIterator

import com.gemstone.gemfire.internal.cache.ExternalTableMetaData
import com.pivotal.gemfirexd.internal.engine.access.heap.MemHeapScanController
import com.pivotal.gemfirexd.internal.engine.store.AbstractCompactExecRow
import com.pivotal.gemfirexd.internal.iapi.store.access.ScanController

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeFormatter, CodegenContext}
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference}
import org.apache.spark.sql.execution.columnar.impl.ColumnFormatRelation
import org.apache.spark.sql.execution.{BufferedRowIterator, CodegenSupport, LeafExecNode, RowTableScan, WholeStageCodegenExec}
import org.apache.spark.sql.store.CodeGeneration
import org.apache.spark.sql.types._

final class ColumnBatchCreator(
    val tableName: String, // internal column table name
    val schema: StructType,
    val externalStore: ExternalStore,
    val compressionCodec: String) extends Logging {

  def createAndStoreBatch(sc: ScanController, row: AbstractCompactExecRow,
      batchID: UUID, bucketID: Int,
      dependents: Seq[ExternalTableMetaData]): java.util.HashSet[AnyRef] = {
    var connectedExternalStore: ConnectedExternalStore = null
    var success: Boolean = false
    try {
      val store = if (dependents.isEmpty) externalStore else {
        connectedExternalStore = externalStore.getConnectedExternalStore(
          tableName, onExecutor = true)
        val indexStatements = dependents.map(ColumnFormatRelation
            .getIndexUpdateStruct(_, connectedExternalStore))
        connectedExternalStore.withDependentAction { _ =>
          indexStatements.foreach(_._2.executeBatch())
        }
      }
      val memHeapScanController = sc.asInstanceOf[MemHeapScanController]
      memHeapScanController.setAddRegionAndKey()
      val keySet = new java.util.HashSet[AnyRef]
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
      try {
        val gen = CodeGeneration.compileCode(tableName + "._BATCH", schema.fields, () => {
          val tableScan = RowTableScan(schema.toAttributes, schema,
            dataRDD = null, numBuckets = -1, partitionColumns = Seq.empty,
            partitionColumnAliases = Seq.empty, baseRelation = null)
          // always create only one column batch hence size is Int.MaxValue
          val insertPlan = ColumnInsertExec(tableScan, overwrite = false,
            Seq.empty, Seq.empty, None, Int.MaxValue, -1, tableName, schema,
            store, useMemberVariables = false, onExecutor = true,
            nonSerialized = false)
          // now generate the code with the help of WholeStageCodegenExec
          // this is only used for local code generation while its RDD semantics
          // and related methods are all ignored
          val (ctx, code) = doCodeGen(WholeStageCodegenExec(insertPlan),
            insertPlan)
          val references = ctx.references
          // also push the index of batchId reference at the end which can be
          // used by caller to update the reference objects before execution
          references += insertPlan.batchIdRef
          (code, references.toArray)
        })
        val references = gen._2
        // update the batchUUID and bucketId as per the passed values
        // the index of the batchId (and bucketId after that) has already
        // been pushed in during compilation above
        val batchIdRef = references(references.length - 1).asInstanceOf[Int]
        references(batchIdRef) = Some(batchID.toString)
        references(batchIdRef + 1) = bucketID
        // no harm in passing a references array with an extra element at end
        val iter = gen._1.generate(references).asInstanceOf[BufferedRowIterator]
        iter.init(0, Array(execRows.asInstanceOf[Iterator[InternalRow]]))
        while (iter.hasNext) {
          iter.next() // ignore result which is number of inserted rows
        }
        keySet
      } finally {
        sc.close()
        success = true
      }
    } finally {
      if (connectedExternalStore != null) {
        connectedExternalStore.commitAndClose(success)
      }
    }
  }

  /**
   * This returns a [[ColumnBatchRowsBuffer]] that can be used for
   * insertion of rows as they appear. Currently used by sampler that
   * does not have any indexes so there is no dependents handling here.
   */
  def createColumnBatchBuffer(columnBatchSize: Int): ColumnBatchRowsBuffer = {
    val gen = CodeGeneration.compileCode(tableName + "._BUFFER", schema.fields, () => {
      val bufferPlan = CallbackColumnInsert(schema)
      val insertPlan = ColumnInsertExec(bufferPlan, overwrite = false,
        Seq.empty, Seq.empty, None, columnBatchSize, -1, tableName, schema,
        externalStore, useMemberVariables = true, onExecutor = true,
        nonSerialized = false)
      // now generate the code with the help of WholeStageCodegenExec
      // this is only used for local code generation while its RDD semantics
      // and related methods are all ignored
      val (ctx, code) = doCodeGen(WholeStageCodegenExec(insertPlan), insertPlan)
      val references = ctx.references.toArray
      (code, references)
    })
    val iter = gen._1.generate(gen._2).asInstanceOf[BufferedRowIterator]
    iter.init(0, Array.empty)
    // get the ColumnBatchRowsBuffer by reflection
    iter.getClass.getMethod("getRowsBuffer").invoke(iter)
        .asInstanceOf[ColumnBatchRowsBuffer]
  }

  /**
   * Generates code for this subtree.
   *
   * Adapted from WholeStageCodegenExec to allow running on executors.
   *
   * @return the tuple of the codegen context and the actual generated source.
   */
  def doCodeGen(plan: CodegenSupport,
      child: CodegenSupportOnExecutor): (CodegenContext, CodeAndComment) = {
    val ctx = new CodegenContext
    val code = child.produceOnExecutor(ctx, plan)
    val source =
      s"""
      public Object generate(Object[] references) {
        return new GeneratedIterator(references);
      }

      ${ctx.registerComment(s"""Codegend pipeline for\n${child.treeString.trim}""")}
      final class GeneratedIterator extends org.apache.spark.sql.execution.BufferedRowIterator {

        private Object[] references;
        ${ctx.declareMutableStates()}

        public GeneratedIterator(Object[] references) {
          this.references = references;
        }

        public void init(int index, scala.collection.Iterator inputs[]) {
          partitionIndex = index;
          ${ctx.initMutableStates()}
        }

        ${ctx.declareAddedFunctions()}

        protected void processNext() throws java.io.IOException {
          ${code.trim}
        }
      }
      """.trim

    // try to compile, helpful for debug
    val cleanedSource = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(CodeFormatter.stripExtraNewLines(source),
        ctx.getPlaceHolderToComments()))

    logDebug(s"\n${CodeFormatter.format(cleanedSource)}")
    (ctx, cleanedSource)
  }
}

/**
 * Allow invoking produce/consume calls on executor without requiring
 * a SparkContext.
 */
trait CodegenSupportOnExecutor extends CodegenSupport {

  /**
   * Returns Java source code to process the rows from input RDD that
   * will work on executors too (assuming no sub-query processing required).
   */
  def produceOnExecutor(ctx: CodegenContext, parent: CodegenSupport): String = {
    this.parent = parent
    ctx.freshNamePrefix = nodeName.toLowerCase
    s"""
       |${ctx.registerComment(s"PRODUCE ON EXECUTOR: ${this.simpleString}")}
       |${doProduce(ctx)}
     """.stripMargin
  }
}

trait ColumnBatchRowsBuffer {

  def startRows(bucketId: Int): Unit

  def appendRow(row: InternalRow): Unit

  def endRows(): Unit
}

case class CallbackColumnInsert(_schema: StructType)
    extends LeafExecNode with CodegenSupportOnExecutor {

  override def output: Seq[Attribute] = _schema.toAttributes

  override lazy val schema: StructType = _schema

  override def inputRDDs(): Seq[RDD[InternalRow]] =
    throw new UnsupportedOperationException("unexpected invocation")

  override protected def doProduce(ctx: CodegenContext): String = {
    val row = ctx.freshName("row")
    val rowsBuffer = ctx.freshName("rowsBuffer")
    val rowsBufferClass = classOf[ColumnBatchRowsBuffer].getName
    ctx.addMutableState(rowsBufferClass, rowsBuffer, "")
    // add bucketId variable set to -1 by default
    val bucketIdTerm = ExternalStoreUtils.COLUMN_BATCH_BUCKETID_VARNAME
    ctx.addMutableState("int", bucketIdTerm, s"$bucketIdTerm = -1;")
    val columnsExpr = output.zipWithIndex.map { case (a, i) =>
      BoundReference(i, a.dataType, a.nullable)
    }
    ctx.INPUT_ROW = row
    ctx.currentVars = null
    val columnsInput = ctx.generateExpressions(columnsExpr)
    ctx.addNewFunction("getRowsBuffer",
      s"""
         |public $rowsBufferClass getRowsBuffer() {
         |  currentRows.clear(); // clear any old results
         |  // initialize the $rowsBuffer
         |  if (this.$rowsBuffer == null) {
         |    processNext();
         |    currentRows.clear(); // clear the accumulated dummy zero result
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
       |    public void startRows(int bucketId) {
       |      // set the bucketId; rest of initialization done in getRowsBuffer
       |      $bucketIdTerm = bucketId;
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
