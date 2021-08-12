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
package org.apache.spark.sql.execution.columnar.impl

import com.gemstone.gemfire.internal.cache.RegionEntry
import com.gemstone.gemfire.internal.cache.persistence.query.CloseableIterator

/**
 * Base trait for iterators that are capable of reading and returning
 * the entire set of columns of a column batch. These can be local region
 * iterators or those fetching entries from remote nodes.
 */
abstract class ClusteredColumnIterator extends CloseableIterator[RegionEntry] {

  /**
   * Get the column value (1-based) for current iterator position. Requires
   * the hasNext and next of iterator to have been invoked first else can
   * throw an NullPointerException.
   */
  def getColumnValue(column: Int): AnyRef
}
