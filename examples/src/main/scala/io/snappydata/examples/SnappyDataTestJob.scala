/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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

package io.snappydata.examples

import com.typesafe.config.Config
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

/**
 * This is a basic job template which can be easily edited to get started with snappydata.
 *
 * If you run the job as is, you can can see testtable, create by the job.
 * To run:
 * `$ ./bin/snappy-job.sh submit --lead localhost:8090 \
 * --app-name SnappyDataTestJob --class io.snappydata.examples.SnappyDataTestJob \
 * --app-jar $SNAPPY_HOME/examples/jars/quickstart.jar`
 *
 * NOTE: You might have to change the jar name and path according to your setup.
 *
 * To check job's result:
 * `$ ./bin/snappy-sql`
 * `snappy-sql> connect client 'localhost:1527';`
 * `snappy-sql> show tables;`
 */

object SnappyDataTestJob extends SnappySQLJob {
  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {

    // Following code can be edited to add your logic.

    import snSession.implicits._
    val tableName = "testtable"
    val dataSet = Seq((1, 2, 3), (4, 5, 6), (7, 8, 9)).toDS()
    snSession.sql(s"DROP TABLE IF EXISTS $tableName")
    snSession.sql(s"CREATE TABLE $tableName (col1 int, col2 int, col3 int)")
    dataSet.write.insertInto(s"$tableName")
    snSession.sql(s"select count(*) from $tableName").show
    // You can check from snappy-sql if the testtable was created successfully.


  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()


}
