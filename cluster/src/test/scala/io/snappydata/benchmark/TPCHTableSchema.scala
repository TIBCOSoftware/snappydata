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

package io.snappydata.benchmark

import java.sql.Date

import org.apache.spark.sql.types.{StructField, StructType}


object TPCHTableSchema {
  case class StreamMessageRegionObject(
      r_regionkey: Int,
      r_name: String,
      r_comment: String
      )

  def  newRegionSchema(schema:StructType): StructType= {
    new StructType (schema.map { a =>
      a match {
        case b if b.name.equals("r_regionkey") => new StructField(b.name, b.dataType, false)
        case c if c.name.equals("r_name") => new StructField(c.name, c.dataType, false)
        case d if d.name.equals("r_comment") => new StructField(d.name, d.dataType, true)
      }
    }.toArray)
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

  def  newNationSchema(schema:StructType): StructType= {
    new StructType (schema.map { a =>
      a match {
        case b if b.name.equals("n_nationkey") => new StructField(b.name, b.dataType, false)
        case c if c.name.equals("n_name") => new StructField(c.name, c.dataType, false)
        case d if d.name.equals("n_regionkey") => new StructField(d.name, d.dataType, false)
        case e if e.name.equals("n_comment") => new StructField(e.name, e.dataType, true)
      }
    }.toArray)
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

  def  newSupplierSchema(schema:StructType): StructType= {
    new StructType (schema.map { a =>
      a match {
        case b if b.name.equals("s_suppkey") => new StructField(b.name, b.dataType, false)
        case c if c.name.equals("s_name") => new StructField(c.name, c.dataType, false)
        case d if d.name.equals("s_address") => new StructField(d.name, d.dataType, false)
        case e if e.name.equals("s_nationkey") => new StructField(e.name, e.dataType, false)
        case f if f.name.equals("s_phone") => new StructField(f.name, f.dataType, false)
        case g if g.name.equals("s_acctbal") => new StructField(g.name, g.dataType, false)
        case i if i.name.equals("s_comment") => new StructField(i.name, i.dataType, false)
      }
    }.toArray)
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
      o_orderkey:Int,
      o_custkey:Int,
      o_orderstatus:String,
      o_totalprice:Double,
      o_orderdate:Date,
      o_orderpriority:String,
      o_clerk:String,
      o_shippriority:Int,
      o_comment:String
      )


  def  newOrderSchema(schema:StructType): StructType= {
    new StructType (schema.map { a =>
      a match {
        case b if b.name.equals("o_orderkey") => new StructField(b.name, b.dataType, false)
        case c if c.name.equals("o_custkey") => new StructField(c.name, c.dataType, false)
        case d if d.name.equals("o_orderstatus") => new StructField(d.name, d.dataType, false)
        case e if e.name.equals("o_totalprice") => new StructField(e.name, e.dataType, false)
        case f if f.name.equals("o_orderdate") => new StructField(f.name, f.dataType, false)
        case g if g.name.equals("o_orderpriority") => new StructField(g.name, g.dataType, false)
        case h if h.name.equals("o_clerk") => new StructField(h.name, h.dataType, false)
        case i if i.name.equals("o_shippriority") => new StructField(i.name, i.dataType, false)
        case j if j.name.equals("o_comment") => new StructField(j.name, j.dataType, false)
      }
    }.toArray)
  }

  def  parseOrderRow(s: Array[String]): StreamMessageOrderObject = {
    StreamMessageOrderObject(
      s(0).toInt,
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
      l_orderkey:Int,
      l_partkey:Int,
      l_suppkey:Int,
      l_linenumber:Int,
      l_quantity:Double,
      l_extendedprice:Double,
      l_discount:Double,
      l_tax:Double,
      l_returnflag:String,
      l_linestatus:String,
      l_shipdate:Date,
      l_commitdate:Date,
      l_receiptdate:Date,
      l_shipinstruct:String,
      l_shipmode:String,
      l_comment:String
      )

  def  newLineItemSchema(schema:StructType): StructType= {
    new StructType (schema.map { a =>
      a match {
        case b if b.name.equals("l_orderkey") => new StructField(b.name, b.dataType, false)
        case c if c.name.equals("l_partkey") => new StructField(c.name, c.dataType, false)
        case d if d.name.equals("l_suppkey") => new StructField(d.name, d.dataType, false)
        case e if e.name.equals("l_linenumber") => new StructField(e.name, e.dataType, false)
        case f if f.name.equals("l_quantity") => new StructField(f.name, f.dataType, false)
        case g if g.name.equals("l_extendedprice") => new StructField(g.name, g.dataType, false)
        case i if i.name.equals("l_discount") => new StructField(i.name, i.dataType, false)
        case j if j.name.equals("l_tax") => new StructField(j.name, j.dataType, false)
        case k if k.name.equals("l_returnflag") => new StructField(k.name, k.dataType, false)
        case l if l.name.equals("l_linestatus") => new StructField(l.name, l.dataType, false)
        case m if m.name.equals("l_shipdate") => new StructField(m.name, m.dataType, false)
        case n if n.name.equals("l_commitdate") => new StructField(n.name, n.dataType, false)
        case o if o.name.equals("l_receiptdate") => new StructField(o.name, o.dataType, false)
        case p if p.name.equals("l_shipinstruct") => new StructField(p.name, p.dataType, false)
        case q if q.name.equals("l_shipmode") => new StructField(q.name, q.dataType, false)
        case s if s.name.equals("l_comment") => new StructField(s.name, s.dataType, false)
      }
    }.toArray)
  }

  def parseLineItemRow(s: Array[String]): StreamMessageLineItemObject = {
    StreamMessageLineItemObject(
      s(0).toInt,
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

  def  newPartSchema(schema:StructType): StructType= {
    new StructType (schema.map { a =>
      a match {
        case b if b.name.equals("p_partkey") => new StructField(b.name, b.dataType, false)
        case c if c.name.equals("p_name") => new StructField(c.name, c.dataType, false)
        case d if d.name.equals("p_mfgr") => new StructField(d.name, d.dataType, false)
        case e if e.name.equals("p_brand") => new StructField(e.name, e.dataType, false)
        case f if f.name.equals("p_type") => new StructField(f.name, f.dataType, false)
        case g if g.name.equals("p_size") => new StructField(g.name, g.dataType, false)
        case i if i.name.equals("p_container") => new StructField(i.name, i.dataType, false)
        case j if j.name.equals("p_retailprice") => new StructField(j.name, j.dataType, false)
        case k if k.name.equals("p_comment") => new StructField(k.name, k.dataType, false)
      }
    }.toArray)
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

  def  newPartSuppSchema(schema:StructType): StructType= {
    new StructType (schema.map { a =>
      a match {
        case b if b.name.equals("ps_partkey") => new StructField(b.name, b.dataType, false)
        case c if c.name.equals("ps_suppkey") => new StructField(c.name, c.dataType, false)
        case d if d.name.equals("ps_availqty") => new StructField(d.name, d.dataType, false)
        case e if e.name.equals("ps_supplycost") => new StructField(e.name, e.dataType, false)
        case f if f.name.equals("ps_comment") => new StructField(f.name, f.dataType, false)
      }
    }.toArray)
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

  def  newCustomerSchema(schema:StructType): StructType= {
    new StructType (schema.map { a =>
      a match {
        case b if b.name.equals("C_CUSTKEY") => new StructField(b.name, b.dataType, false)
        case c if c.name.equals("C_NAME") => new StructField(c.name, c.dataType, false)
        case d if d.name.equals("C_ADDRESS") => new StructField(d.name, d.dataType, false)
        case e if e.name.equals("C_NATIONKEY") => new StructField(e.name, e.dataType, false)
        case f if f.name.equals("C_PHONE") => new StructField(f.name, f.dataType, false)
        case g if g.name.equals("C_ACCTBAL") => new StructField(g.name, g.dataType, false)
        case i if i.name.equals("C_MKTSEGMENT") => new StructField(i.name, i.dataType, false)
        case j if j.name.equals("C_COMMENT") => new StructField(j.name, j.dataType, false)
      }
    }.toArray)
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
  def formatDate(dateString:String): Date = {
    val splittedDate = dateString.split("-")
    val year = splittedDate(0)
    val month = splittedDate(1)
    val day= splittedDate(2)
    new Date((year.toInt - 1900), (month.toInt -1), day.toInt)
  }
}
