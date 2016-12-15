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
package io.snappydata.examples

sealed trait Setup
case class SnappyShellSetup(sqlCommand :Seq[String]) extends Setup
case class BashShellSetup(shellCommand : String) extends Setup

sealed trait Example{
  def name : String
}
case class Job(override val name : String, jobClass : String, setup: Setup) extends Example
case class App(override val name :String , appClass : String, setup: Setup) extends Example


object ExampleConf {

  val examples: Seq[Example] = Seq(

    new Job("airlineDataJob",
    "io.snappydata.examples.AirlineDataJob",
      SnappyShellSetup(Seq("connect client 'localhost:1527';",
        "run 'quickstart/scripts/create_and_load_column_table.sql';",
        "run 'quickstart/scripts/create_and_load_row_table.sql';",
        "run 'quickstart/scripts/create_and_load_sample_table.sql';",
        "run 'quickstart/scripts/status_queries.sql';",
        "run 'quickstart/scripts/olap_queries.sql';",
        "run 'quickstart/scripts/oltp_queries.sql';",
        "run 'quickstart/scripts/olap_queries.sql';",
        "run 'quickstart/scripts/olap_approx_queries.sql';",
        "exit;") )),

  new Job("CreateAndLoadAirlineDataJob",
    "io.snappydata.examples.CreateAndLoadAirlineDataJob",null)

  )

}
