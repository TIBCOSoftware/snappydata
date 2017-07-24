/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

package org.apache.spark.sql.execution.columnar.encoding

import org.apache.spark.sql.types.{DataType, StructField}

/**
 * Decodes a column of a batch that has seen some changes (updates or deletes) by
 * combining all delta values, delete bitmask and full column value obtained from
 * [[ColumnDeltaEncoder]]s and column encoders. Callers should
 * provide this with the set of all deltas and delete bitmask for the column
 * apart from the full column value, then use it like a normal decoder.
 */
final class MutatedColumnDecoder(decoder: ColumnDecoder, delta1: ColumnDeltaDecoder,
    delta2: ColumnDeltaDecoder, delta3: ColumnDeltaDecoder) extends ColumnDecoder {

  override def typeId: Int = decoder.typeId

  override def supports(dataType: DataType): Boolean = decoder.supports(dataType)

  override protected def hasNulls: Boolean =
    throw new UnsupportedOperationException(s"hasNulls for $toString")

  override protected[sql] def initializeNulls(columnBytes: AnyRef, cursor: Long,
      field: StructField): Long = {
    decoder.initializeNulls(columnBytes, cursor, field)
    delta1.initializeNulls(columnBytes, cursor, field)
  }

  override protected[sql] def initializeCursor(columnBytes: AnyRef, cursor: Long,
      field: StructField): Long = {
  }
}
