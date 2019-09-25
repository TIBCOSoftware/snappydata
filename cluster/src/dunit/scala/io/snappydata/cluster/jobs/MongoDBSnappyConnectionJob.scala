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
package io.snappydata.cluster.jobs

import com.typesafe.config.Config
import org.apache.spark.sql._

object MongoDBSnappyConnectionJob extends SnappySQLJob{

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValid = SnappyJobValid()

  override def runSnappyJob(sc: SnappySession, jobConfig: Config): Any = {
    // scalastyle:off println
    val df = sc.read.format("com.mongodb.spark.sql.DefaultSource")
        .option("spark.mongodb.input.uri", "mongodb://localhost:23456/employeesDB.employees")
        .option("spark.mongodb.input.database", "employeesDB")
        .option("spark.mongodb.input.collection", "employees")
        .option("spark.mongodb.output.uri", "mongodb://127.0.0.1/employeesDB.employees")
        .option("spark.mongodb.output.database", "employeesDB")
        .option("spark.mongodb.output.collection", "employees")
        .option("spark.mongodb.input.partitioner", "MongoPaginateByCountPartitioner")
        .option("spark.mongodb.input.partitionerOptions." +
            "MongoPaginateByCountPartitioner.numberOfPartitions", "1")
        .option("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
        .load()
    df.show()
    df.write.format("column").mode(SaveMode.Overwrite).saveAsTable("employees")
    val showDF = sc.sql("select * from employees").collect()
    println("Printing the contents of the employees table")
    showDF.foreach(println)
  }
}
