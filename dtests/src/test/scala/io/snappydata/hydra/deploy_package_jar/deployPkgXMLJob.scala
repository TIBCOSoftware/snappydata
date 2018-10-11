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
package io.snappydata.hydra.deploy_package_jar

import com.pivotal.gemfirexd.internal.engine.Misc
import com.typesafe.config.Config
import org.apache.spark.sql._

class deployPkgXMLJob extends SnappySQLJob{
  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {

    val xmlFilePath: String = jobConfig.getString("xmlFileLocation")

    val xmlSchema = snappySession.read.format("com.databricks.spark.xml")
      .option("rowTag", "catalog")
      .load(s"${xmlFilePath}")

    val schemaGeneration = xmlSchema.schema

    val xmlData = snappySession.read.format("com.databricks.spark.xml")
      .option("rowTag", "catalog").schema(schemaGeneration)
      .load(s"${xmlFilePath}")

    xmlData.createOrReplaceTempView("tempXMLTbl")

    // TODO Below statement to be tested, on ROW TABLE AS SELECT has problem and It is know Issue.
    // snappySession.sql("CREATE TABLE Books AS SELECT * FROM tempXMLTbl using column options()")

    snappySession.sql("CREATE TABLE Books using column AS SELECT * FROM tempXMLTbl")

    val r = snappySession.sql("SELECT * FROM Books").collect()

    val logger = Misc.getCacheLogWriterNoThrow

    if (logger != null) {
      logger.info("Result = " + r + " and size = " + r.size +
        " and first element = " + r(0))
    }
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}
