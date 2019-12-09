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
package org.apache.spark.sql

import io.snappydata.util.ServiceUtils

import org.apache.spark.sql.jdbc.JDBCSuite
import org.apache.spark.sql.test.{SharedSnappySessionContext, SnappySparkTestUtil}

class SnappyJDBCSuite extends JDBCSuite
    with SharedSnappySessionContext with SnappySparkTestUtil {
  test("hide credentials in captured ddl string") {
    // jdbc connection strings from diffreent databases
    val jdbcUrls = Seq(
      // jdbc pattern 1
      "jdbc:mysql://[(host=myhost1,port=1111,user=sandy," +
          "password=secret),(host=myhost2,port=2222,user=finn,password=secret)]/db",
      "jdbc:mysql://address=(host=myhost1)(port=1111)(user=sandy)(password=secret)," +
          "address=(host=myhost2)(port=2222)(user=finn)(password=secret)/db",
      "jdbc:mysql://localhost/test?user=minty&password=greatsqldb",
      "jdbc:postgresql://localhost/test?user=fred&password=secret&ssl=true",
      "jdbc:postgresql://localhost/mydb?user=other&password=secret",
      "jdbc:h2:file:~/sample;USER=sa;PASSWORD=123",
      "jdbc:derby://localhost:1527/myDB;create=true;user=me;password=mine",
      "jdbc:derby:myDB;create=true;user=me;password=mine",
      // jdbc pattern2
      "jdbc:mysql://sandy:secret@[myhost1:1111,myhost2:2222]/db",
      "jdbc:mysql://sandy:secret@[(address=host1:1111,priority=1,key1=value1)," +
          "(address=host2:2222,priority=2,key2=value2))]/db",
      // s3 path pattern1
      "s3a://DUMMYKEY175GDRZIF4QQ:DUMMYKEY2zUkvIS88xrMJ7v5cMmQEWRjqS@" +
          "ryft-public-sample-data/passengers.txt",
      // no password
      "jdbc:mysql://localhost/test",
      // jdbc pattern and option pattern
      s"""
         |CREATE table jdbctable
         |USING org.apache.spark.sql.jdbc
         |OPTIONS (
         | url 'jdbc:h2:mem:testdb0;user=testUser;password=testPass',
         | dbtable 'TEST.PEOPLE',
         | user 'testUser',
         | password 'myanotherpass')""".stripMargin
    )
    val expectedUrls = Seq("jdbc:mysql://[(host=myhost1,port=1111,user=sandy," +
        "password=xxxxx),(host=myhost2,port=2222,user=finn,password=xxxxx)]/db",
      "jdbc:mysql://address=(host=myhost1)(port=1111)(user=sandy)(password=xxxxx)," +
          "address=(host=myhost2)(port=2222)(user=finn)(password=xxxxx)/db",
      "jdbc:mysql://localhost/test?user=minty&password=xxxxx",
      "jdbc:postgresql://localhost/test?user=fred&password=xxxxx&ssl=true",
      "jdbc:postgresql://localhost/mydb?user=other&password=xxxxx",
      "jdbc:h2:file:~/sample;USER=sa;PASSWORD=xxxxx",
      "jdbc:derby://localhost:1527/myDB;create=true;user=me;password=xxxxx",
      "jdbc:derby:myDB;create=true;user=me;password=xxxxx",
      "jdbc:mysql://sandy:xxxxx@[myhost1:1111,myhost2:2222]/db",
      "jdbc:mysql://sandy:xxxxx@[(address=host1:1111,priority=1,key1=value1)," +
          "(address=host2:2222,priority=2,key2=value2))]/db",
      "s3a://xxxxx:xxxxx@ryft-public-sample-data/passengers.txt",
      "jdbc:mysql://localhost/test",
      " CREATE table jdbctable USING org.apache.spark.sql.jdbc OPTIONS (  url " +
          "'jdbc:h2:mem:testdb0;user=testUser;password=xxxxx',  dbtable 'TEST.PEOPLE'," +
          "  user 'testUser',  password 'xxxxx')"
    )
    val maskedUrls = jdbcUrls.map(url => ServiceUtils.maskPasswordsInString(url))
    jdbcUrls.indices.foreach(i => assert(maskedUrls(i).equals(expectedUrls(i)),
      s"password in url not masked. url=${maskedUrls(i)}"))
  }
}
