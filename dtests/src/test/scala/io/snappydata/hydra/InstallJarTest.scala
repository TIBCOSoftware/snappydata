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
package io.snappydata.hydra

import com.typesafe.config.Config
import io.snappydata.hydra.installJar.DynamicJarLoadingTest
import org.apache.spark.sql._

class InstallJarTest extends SnappySQLJob {
  override def runSnappyJob(snc: SnappyContext, jobConfig: Config): Any = {
    DynamicJarLoadingTest.verify(snc, jobConfig.getString("classVersion"))
  }
  override def isValidJob(sc: SnappyContext, config: Config): SnappyJobValidation = SnappyJobValid()
}
