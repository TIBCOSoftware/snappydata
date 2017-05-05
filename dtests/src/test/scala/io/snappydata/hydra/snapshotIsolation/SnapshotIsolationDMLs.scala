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

package io.snappydata.hydra.snapshotIsolation

object SnapshotIsolationDMLs {

  val insert1: String = "insert into employees values(10,'Smith','D','Sales Representative','Mr.'," +
      "'1966-01-27 00:00:00.000','1994-11-15 00:00:00.000','Houndstooth Rd.,London',NULL,'WG2 7LT'," +
      "'UK','(71) 555-4444','452',NULL,'Smith has a BA degree in English from St. Lawrence College.'," +
      "5,'http://accweb/emmployees/davolio.bmp')"

  val inserts = List  (
  "insert1" -> insert1
  )
}
