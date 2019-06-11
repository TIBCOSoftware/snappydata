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

package io.snappydata.benchmark

import java.sql.Date

import org.apache.spark.sql.types.StructType


object TPCHTableSchema {

  case class StreamMessageRegionObject(
      r_regionkey: Int,
      r_name: String,
      r_comment: String
  )

  def newRegionSchema(schema: StructType): StructType = {
    new StructType(schema.map(_.copy(nullable = false)).toArray)
  }

  def parseRegionRow(s: Array[String]): StreamMessageRegionObject = {
    StreamMessageRegionObject(
      s(0).toInt,
      s(1),
      s(2)
    )
  }

  case class StreamMessageNationObject(
      n_nationkey: Int,
      n_name: String,
      n_regionkey: Int,
      n_comment: String
  )

  def newNationSchema(schema: StructType): StructType = {
    new StructType(schema.map(_.copy(nullable = false)).toArray)
  }

  def parseNationRow(s: Array[String]): StreamMessageNationObject = {
    StreamMessageNationObject(
      s(0).toInt,
      s(1),
      s(2).toInt,
      s(3)
    )
  }

  case class StreamMessageSupplierObject(
      s_suppkey: Int,
      s_name: String,
      s_address: String,
      s_nationkey: Int,
      s_phone: String,
      s_acctbal: Double,
      s_comment: String
  )

  def newSupplierSchema(schema: StructType): StructType = {
    new StructType(schema.map(_.copy(nullable = false)).toArray)
  }

  def parseSupplierRow(s: Array[String]): StreamMessageSupplierObject = {
    StreamMessageSupplierObject(
      s(0).toInt,
      s(1),
      s(2),
      s(3).toInt,
      s(4),
      s(5).toDouble,
      s(6)
    )
  }

  case class StreamMessageOrderObject(
      o_orderkey: Long,
      o_custkey: Int,
      o_orderstatus: String,
      o_totalprice: Double,
      o_orderdate: Date,
      o_orderpriority: String,
      o_clerk: String,
      o_shippriority: Int,
      o_comment: String
  )


  def newOrderSchema(schema: StructType): StructType = {
    new StructType(schema.map(_.copy(nullable = false)).toArray)
  }

  def parseOrderRow(s: Array[String]): StreamMessageOrderObject = {
    StreamMessageOrderObject(
      s(0).toLong,
      s(1).toInt,
      s(2),
      s(3).toDouble,
      formatDate(s(4)),
      s(5),
      s(6),
      s(7).toInt,
      s(8)
    )
  }

  case class StreamMessageLineItemObject(
      l_orderkey: Long,
      l_partkey: Int,
      l_suppkey: Int,
      l_linenumber: Int,
      l_quantity: Double,
      l_extendedprice: Double,
      l_discount: Double,
      l_tax: Double,
      l_returnflag: String,
      l_linestatus: String,
      l_shipdate: Date,
      l_commitdate: Date,
      l_receiptdate: Date,
      l_shipinstruct: String,
      l_shipmode: String,
      l_comment: String
  )

  def newLineItemSchema(schema: StructType): StructType = {
    new StructType(schema.map(_.copy(nullable = false)).toArray)
  }

  def parseLineItemRow(s: Array[String]): StreamMessageLineItemObject = {
    StreamMessageLineItemObject(
      s(0).toLong,
      s(1).toInt,
      s(2).toInt,
      s(3).toInt,
      s(4).toDouble,
      s(5).toDouble,
      s(6).toDouble,
      s(7).toDouble,
      s(8),
      s(9),
      formatDate(s(10)),
      formatDate(s(11)),
      formatDate(s(12)),
      s(13),
      s(14),
      s(15)
    )
  }

  case class StreamMessagePartObject(
      p_partkey: Int,
      p_name: String,
      p_mfgr: String,
      p_brand: String,
      p_type: String,
      p_size: Int,
      p_container: String,
      p_retailprice: Double,
      p_comment: String
  )

  def newPartSchema(schema: StructType): StructType = {
    new StructType(schema.map(_.copy(nullable = false)).toArray)
  }

  def parsePartRow(s: Array[String]): StreamMessagePartObject = {
    StreamMessagePartObject(
      s(0).toInt,
      s(1),
      s(2),
      s(3),
      s(4),
      s(5).toInt,
      s(6),
      s(7).toDouble,
      s(8)
    )
  }

  case class StreamMessagePartSuppObject(
      ps_partkey: Int,
      ps_suppkey: Int,
      ps_availqty: Int,
      ps_supplycost: Double,
      ps_comment: String
  )

  def newPartSuppSchema(schema: StructType): StructType = {
    new StructType(schema.map(_.copy(nullable = false)).toArray)
  }

  def parsePartSuppRow(s: Array[String]): StreamMessagePartSuppObject = {
    StreamMessagePartSuppObject(
      s(0).toInt,
      s(1).toInt,
      s(2).toInt,
      s(3).toDouble,
      s(4)
    )
  }

  case class StreamMessageCustomerObject(
      C_CUSTKEY: Int,
      C_NAME: String,
      C_ADDRESS: String,
      C_NATIONKEY: Int,
      C_PHONE: String,
      C_ACCTBAL: Double,
      C_MKTSEGMENT: String,
      C_COMMENT: String
  )

  def newCustomerSchema(schema: StructType): StructType = {
    new StructType(schema.map(_.copy(nullable = false)).toArray)
  }

  def parseCustomerRow(s: Array[String]): StreamMessageCustomerObject = {
    StreamMessageCustomerObject(
      s(0).toInt,
      s(1),
      s(2),
      s(3).toInt,
      s(4),
      s(5).toDouble,
      s(6),
      s(7)
    )
  }

  def formatDate(dateString: String): Date = {
    val splittedDate = dateString.split("-")
    val year = splittedDate(0)
    val month = splittedDate(1)
    val day = splittedDate(2)
    new Date(year.toInt - 1900, month.toInt - 1, day.toInt)
  }
}
