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

package org.apache.spark.sql.execution.columnar.impl

/**
 * Encodes a delta value for a [[ColumnFormatValue]] obtained after an
 * update operation. This keeps values in partially encoded form (e.g.
 * for dictionary encoding, the dictionary and indexes will be separate) and
 * not in the final form for best performance in update (at the cost of a slight
 * performance overhead in scan in certain situations).
 *
 * The design is straightforward one where the updated values of each column
 * are held in this object with their ordinals kept separately encoded as a
 * list of positions. A new updated column value results in the addition of new
 * entry in the (partially) encoded values held in the corresponding delta.
 * Each delta can grow to a limit after which it is subsumed in a larger delta
 * of bigger size thus creating a hierarchy of deltas. So base delta will go
 * till 100 entries or so, then the next higher level one will go till 1000 entries
 * and so on till the full [[ColumnFormatValue]] size is attained. This design
 * attempts to minimize disk writes at the cost of some scan overhead for
 * columns that see a large number of updates. The hierarchy is expected to be
 * small not more than 4 levels to get a good balance between write overhead
 * and scan overhead.
 *
 * An alternative to having list of positions would be to write position and
 * then the encoded value for each value. This might give a small benefit in
 * scans since there is only one array to be read. However, it has a couple of
 * issues due to which it was not chosen: a) some encoders like RunLength will
 * not be able to encode in that format, b) requires changes to all encoders
 * and decoders to optionally support positions in the middle. This might be
 * considered in future depending on how much performance improvement can be
 * attained (after checking with some testing) for encoders that can support.
 */
object ColumnDelta {

  /**
   * The initial size of delta column (the smallest delta in the hierarchy).
   */
  val INIT_SIZE = 100

  /**
   * The maximum depth of the hierarchy of deltas for column starting with
   * smallest delta, which is merged with larger delta, then larger, ...
   * till the full column value.
   */
  val MAX_DEPTH = 3
}
