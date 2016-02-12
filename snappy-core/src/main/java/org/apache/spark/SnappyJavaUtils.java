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
package org.apache.spark;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameJavaFunctions;


public class SnappyJavaUtils {

  /**
  *
   * A static factory method to create a {@link org.apache.spark.sql.DataFrameJavaFunctions} based on an existing {@link
   * DataFrame} instance.
   */
  public static DataFrameJavaFunctions snappyApis(DataFrame dataFrame) {
    return new DataFrameJavaFunctions(dataFrame);
  }

}
