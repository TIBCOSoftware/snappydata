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
package org.apache.spark.executor

import com.gemstone.gemfire.GemFireException
import com.gemstone.gemfire.cache.CacheClosedException
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException
import org.apache.spark.{SparkCallBackFactory, SnappySparkCallback}


object SnappySparkCallbacksImpl extends SnappySparkCallback {
  override def checkCacheClosing(t: Throwable): Boolean = {

    try {
      Misc.checkIfCacheClosing(t)
    } catch {
      case ex: CacheClosedException => true
      case _ => false
    }
    return false;
  }

  /**
   * This method check if the exeception task can be retried based on the exception
   * @param t
   * @return
   */
  override def checkRuntimeOrGemfireException(t: Throwable): Boolean = {

      if(GemFireXDUtils.retryToBeDone(t)) {
        return true;
      } else {
        return false;
      }
  }
}

trait SparkCallBack extends Serializable {
  // Register callback
  SparkCallBackFactory.setSnappySparkCallback(SnappySparkCallbacksImpl)
}
