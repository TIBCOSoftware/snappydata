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
package io.snappydata.datasource.v2

object V2Constants {

  val DATASOURCE_SHORT_NAME = "snappydata"

  val KEY_PREFIX = "snappydata"

  /*
  TODO: Same property defined in io.snappydata.Property.SnappyConnection
  Move Literals.scala to a shared jar accessible here
  */
  val SnappyConnection = "snappydata.connection"

  val TABLE_NAME = "table"

  val USER = "user"

  val PASSWORD = "password"
}
