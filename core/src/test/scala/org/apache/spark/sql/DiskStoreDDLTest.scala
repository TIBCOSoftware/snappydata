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

package org.apache.spark.sql

import io.snappydata.SnappyFunSuite
import org.scalatest.BeforeAndAfter
import org.apache.spark.Logging

class DiskStoreDDLTest extends SnappyFunSuite
    with Logging with BeforeAndAfter {

  test("test diskstore ddl") {
    snc.sql("CREATE DISKSTORE diskstore_name ('dir1' 10240)")
    snc.sql("create table dummy (id string) using column options( DISKSTORE 'diskstore_name')")
    snc.sql("drop table dummy")
    snc.sql("DROP DISKSTORE diskstore_name")
  }

  test("test call sql syntax"){
    snc.sql("CALL sys.rebalance_all_buckets()")
  }
}
