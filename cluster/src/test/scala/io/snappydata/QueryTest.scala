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

package io.snappydata

import scala.collection.JavaConverters._

import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

class QueryTest extends SnappyFunSuite {

  test("Test exists in select") {
    val snContext = org.apache.spark.sql.SnappyContext(sc)

    snContext.sql("CREATE TABLE titles(title_id varchar(20), title varchar(80) " +
        "not null, type varchar(12) not null, pub_id varchar(4), price int not null, " +
        "advance int not null , royalty int , ytd_sales int,notes varchar(200))")

    snContext.sql("insert into titles values ('1', 'Secrets', " +
        "'popular_comp', '1389', 20, 8000, 10, 4095,'Note 1')")
    snContext.sql("insert into titles values ('2', 'The', " +
        "'business',     '1389', 19, 5000, 10, 4095,'Note 2')")
    snContext.sql("insert into titles values ('3', 'Emotional', " +
        "'psychology',   '0736', 7,  4000, 10, 3336,'Note 3')")
    snContext.sql("insert into titles values ('4', 'Prolonged', " +
        "'psychology',   '0736', 19, 2000, 10, 4072,'Note 4')")
    snContext.sql("insert into titles values ('5', 'With', " +
        "'business',     '1389', 11, 5000, 10, 3876,'Note 5')")
    snContext.sql("insert into titles values ('6', 'Valley', " +
        "'mod_cook',     '0877', 9,  0,    12, 2032,'Note 6')")
    snContext.sql("insert into titles values ('7', 'Any?', " +
        "'trad_cook',    '0877', 14, 8000, 10, 4095,'Note 7')")
    snContext.sql("insert into titles values ('8', 'Fifty', " +
        "'trad_cook',    '0877', 11, 4000, 14, 1509,'Note 8')")

    snContext.sql("CREATE TABLE sales(stor_id varchar(4) not null, " +
        "ord_num varchar(20) not null, qty int not null, " +
        "payterms varchar(12) not null,title_id varchar(80))")

    snContext.sql("insert into sales values('1', 'QA7442.3',  75, 'ON Billing','1')")
    snContext.sql("insert into sales values('2', 'D4482',     10, 'Net 60',    '1')")
    snContext.sql("insert into sales values('3', 'N914008',   20, 'Net 30',    '2')")
    snContext.sql("insert into sales values('4', 'N914014',   25, 'Net 30',    '3')")
    snContext.sql("insert into sales values('5', '423LL922',  15, 'ON Billing','3')")
    snContext.sql("insert into sales values('6', '423LL930',  10, 'ON Billing','2')")

    val df = snContext.sql("SELECT  title, price FROM titles WHERE EXISTS (" +
        "SELECT * FROM sales WHERE sales.title_id = titles.title_id AND qty >30)")

    df.show()
  }

  test("SNAP-1159") {
    val session = SnappyContext(sc).snappySession
    session.sql(s"set ${SQLConf.COLUMN_BATCH_SIZE.key}=10")
    session.sql(s"set ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key}=1")
    val data1 = session.range(20).selectExpr("id")
    val data2 = session.range(80).selectExpr("id", "cast ((id / 4) as long) as k",
      "(case when (id % 4) < 2 then cast((id % 4) as long) else null end) as v")
    data1.write.format("column").saveAsTable("t1")
    data2.write.format("column").saveAsTable("t2")

    SparkSession.clearActiveSession()
    val spark = SparkSession.builder().getOrCreate()
    val sdata1 = spark.range(20).selectExpr("id")
    val sdata2 = spark.createDataFrame(data2.collect().toSeq.asJava, data2.schema)
    sdata1.createOrReplaceTempView("t1")
    sdata2.createOrReplaceTempView("t2")

    val query = "select k, v from t1 inner join t2 where t1.id = t2.k order by k, v"
    val df = session.sql(query)
    val result1 = df.collect().mkString(" ")
    val result2 = spark.sql(query).collect().mkString(" ")
    if (result1 != result2) {
      fail(s"Expected result: $result2\nGot: $result1")
    }
  }
}
