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
package org.apache.spark.sql.policy

import java.sql.SQLException

import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.internal.iapi.error.StandardException
import org.junit.Assert.assertEquals

import org.apache.spark.SparkConf
import org.apache.spark.sql.SnappyContext

class SecurityEnabledPolicyTest extends PolicyTestBase {

  val user1 = "gemfire1"
  val user2 = "gemfire2"

  val props = Map.empty[String, String]
  val tableOwner: String = user1
  val numElements = 100
  val colTableName: String = s"$tableOwner.ColumnTable"
  val rowTableName: String = s"$tableOwner.RowTable"
  var ownerContext: SnappyContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val seq = for (i <- 0 until numElements) yield {
      (s"name_$i", i)
    }
    val rdd = sc.parallelize(seq)
    ownerContext = snc.newSession()
    ownerContext.snappySession.conf.set(Attribute.USERNAME_ATTR, tableOwner)
    ownerContext.snappySession.conf.set(Attribute.PASSWORD_ATTR, tableOwner)
    val dataDF = ownerContext.createDataFrame(rdd)

    ownerContext.sql(s"CREATE TABLE $colTableName (name String, id Int) " +
        s" USING column ")

    ownerContext.sql(s"CREATE TABLE $rowTableName (name String, id Int) " +
        s" USING row ")
    dataDF.write.insertInto(colTableName)
    dataDF.write.insertInto(rowTableName)
  }

  protected override def newSparkConf(addOn: (SparkConf) => SparkConf): SparkConf = {
    newLDAPSparkConf(addOn)
  }

  override def afterAll(): Unit = {
    ownerContext.dropTable(colTableName, ifExists = true)
    ownerContext.dropTable(rowTableName, ifExists = true)
    super.afterAll()
  }


  test("Check only owner of the table can create policy and drop it") {
    val snc2 = snc.newSession()
    snc2.snappySession.conf.set(Attribute.USERNAME_ATTR, user2)
    snc2.snappySession.conf.set(Attribute.PASSWORD_ATTR, user2)
    try {
      snc2.sql(s"create policy testPolicy2 on  " +
          s"$colTableName for select to current_user using id > 10")
      fail("Only owner of the table should be allowed to create policy on it")
    } catch {
      case _: SQLException =>
      case _: StandardException =>
    }

    ownerContext.sql(s"create policy testPolicy2 on  " +
        s"$colTableName for select to current_user using id > 10")

    try {
      snc2.sql(s"drop policy $tableOwner.testPolicy2")
      fail("Only owner of the Policy can drop the policy")
    } catch {
      case _: SQLException =>
      case _: StandardException =>
    }

    ownerContext.sql("drop policy testPolicy2")
  }

  test("check policy applied to ldap group") {
    // the ldap group gemGroup2 contains gemfire3, gemfire4, gemfire5
    ownerContext.sql(s"create policy testPolicy1 on  " +
        s"$colTableName for select to ldapGroup:gemGroup2, gemfire6 using id > 90")

    ownerContext.sql(s"alter table $colTableName enable row level security")

    ownerContext.sql(s"GRANT select ON TABLE  $colTableName TO ldapGroup:gemGroup2," +
        s" gemfire6, gemfire7, gemfire2")

    val snc2 = snc.newSession()
    snc2.snappySession.conf.set(Attribute.USERNAME_ATTR, user2)
    snc2.snappySession.conf.set(Attribute.PASSWORD_ATTR, user2)
    var rs = snc2.sql(s"select * from $colTableName")
    assertEquals(numElements, rs.collect().length)

    val snc3 = snc.newSession()
    snc3.snappySession.conf.set(Attribute.USERNAME_ATTR, "gemfire3")
    snc3.snappySession.conf.set(Attribute.PASSWORD_ATTR, "gemfire3")
    rs = snc3.sql(s"select * from $colTableName")
    assertEquals(9, rs.collect().length)

    val snc4 = snc.newSession()
    snc4.snappySession.conf.set(Attribute.USERNAME_ATTR, "gemfire4")
    snc4.snappySession.conf.set(Attribute.PASSWORD_ATTR, "gemfire4")
    rs = snc4.sql(s"select * from $colTableName")
    assertEquals(9, rs.collect().length)

    val snc5 = snc.newSession()
    snc5.snappySession.conf.set(Attribute.USERNAME_ATTR, "gemfire5")
    snc5.snappySession.conf.set(Attribute.PASSWORD_ATTR, "gemfire5")
    rs = snc5.sql(s"select * from $colTableName")
    assertEquals(9, rs.collect().length)

    val snc6 = snc.newSession()
    snc6.snappySession.conf.set(Attribute.USERNAME_ATTR, "gemfire6")
    snc6.snappySession.conf.set(Attribute.PASSWORD_ATTR, "gemfire6")
    rs = snc6.sql(s"select * from $colTableName")
    assertEquals(9, rs.collect().length)

    val snc7 = snc.newSession()
    snc7.snappySession.conf.set(Attribute.USERNAME_ATTR, "gemfire7")
    snc7.snappySession.conf.set(Attribute.PASSWORD_ATTR, "gemfire7")
    rs = snc7.sql(s"select * from $colTableName")
    assertEquals(numElements, rs.collect().length)


    rs = ownerContext.sql(s"select * from $colTableName")
    assertEquals(numElements, rs.collect().length)
  }

  test("test sql function CURRENT_USER_LDAP_GROUPS()") {
    val snc3 = snc.newSession()
    snc3.snappySession.conf.set(Attribute.USERNAME_ATTR, "gemfire3")
    snc3.snappySession.conf.set(Attribute.PASSWORD_ATTR, "gemfire3")
    snc3.sql(s"CREATE TABLE temp (grp String) " +
        s" USING row ")
    snc3.sql("insert into temp values ('gemGroup1')," +
        "('gemGroup2'), ('gemGroup3')")

    val rs = snc3.sql("select * from temp where " +
        "array_contains(current_user_ldap_groups(), upper(grp))")
    assertEquals(3, rs.collect().length)
    snc3.sql("drop table if exists temp")

  }

}
