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

import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.internal.engine.Misc
import org.junit.Assert.{assertEquals, assertTrue}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Literal}
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

class PolicyTest extends PolicyTestBase {

  val props = Map.empty[String, String]
  val tableOwner = "ashahid"
  val numElements = 100
  val colTableName: String = s"$tableOwner.ColumnTable"
  val rowTableName: String = s"$tableOwner.RowTable"
  var ownerContext: SnappyContext = _

  override protected def systemUser: String = tableOwner

  protected override def newSparkConf(addOn: SparkConf => SparkConf): SparkConf = {
    val conf = new org.apache.spark.SparkConf()
        .setAppName("PolicyTest")
        .setMaster("local[4]")
        .set("spark.sql.crossJoin.enabled", "true")
    if (addOn != null) {
      addOn(conf)
    } else {
      conf
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    val seq = for (i <- 0 until numElements) yield {
      (s"name_$i", i)
    }
    val rdd = sc.parallelize(seq)
    ownerContext = snc.newSession()
    ownerContext.snappySession.conf.set(Attribute.USERNAME_ATTR, tableOwner)

    val dataDF = ownerContext.createDataFrame(rdd)

    ownerContext.sql(s"CREATE TABLE $colTableName (name String, id Int) " +
        s" USING column ")

    ownerContext.sql(s"CREATE TABLE $rowTableName (name String, id Int) " +
        s" USING row ")
    dataDF.write.insertInto(colTableName)
    dataDF.write.insertInto(rowTableName)
    ownerContext.sql(s"alter table $colTableName enable row level security")
    ownerContext.sql(s"alter table $rowTableName enable row level security")
  }

  override def afterAll(): Unit = {
    ownerContext.dropTable(colTableName, ifExists = true)
    ownerContext.dropTable(rowTableName, ifExists = true)
    super.afterAll()
  }

  test("Policy creation on a column table using snappy context") {
    this.testPolicy(colTableName)
  }

  test("Policy creation on a row table using snappy context") {
    this.testPolicy(rowTableName)
  }

  private def testPolicy(tableName: String) {
    ownerContext.sql(s"create policy testPolicy1 on  " +
        s"$tableName for select to current_user using id < 0")
    var rs = ownerContext.sql(s"select * from $tableName").collect()
    assertEquals(numElements, rs.length)

    val snc2 = snc.newSession()
    snc2.snappySession.conf.set(Attribute.USERNAME_ATTR, "UserX")

    rs = snc2.sql(s"select * from $tableName").collect()
    assertEquals(0, rs.length)
    ownerContext.sql("drop policy testPolicy1")
  }

  test("Check Policy Filter applied to the plan only once") {
    ownerContext.sql(s"create policy testPolicy2 on  " +
        s"$colTableName for select to current_user using id > 0")

    val snc2 = snc.newSession()
    snc2.snappySession.conf.set(Attribute.USERNAME_ATTR, "UserX")

    val df = snc2.sql(s"select * from $colTableName")
    val allFilters = df.queryExecution.analyzed.collect {
      case f: Filter => f
    }
    assertEquals(1, allFilters.map(_.condition).flatMap(ex => {
      ex.collect {
        case x@EqualTo(Literal(l1, StringType), Literal(l2, StringType))
          if l1.equals(UTF8String.fromString(PolicyProperties.rlsConditionString)) &&
              l2.equals(UTF8String.fromString(PolicyProperties.rlsConditionString)) => x
      }
    }).length)

    ownerContext.sql("drop policy testPolicy2")
  }

  test("test multiple policies application using snappy context on column table") {
    this.testMultiplePolicy(colTableName)
  }

  test("test multiple policies application using snappy context on row table") {
    this.testMultiplePolicy(rowTableName)
  }

  test("old query plan invalidation on creation of policy on column table using snappy context") {
    this.testQueryPlanInvalidation(colTableName)
  }

  test("old query plan invalidation on creation of policy on row table using snappy context") {
    this.testQueryPlanInvalidation(rowTableName)
  }

  private def testQueryPlanInvalidation(tableName: String): Unit = {

    val snc2 = snc.newSession()
    snc2.snappySession.conf.set(Attribute.USERNAME_ATTR, "UserX")
    val q = s"select * from $tableName where id > 70"
    var rs = snc2.sql(q)
    assertEquals(29, rs.collect().length)
    // fire again
    rs = snc2.sql(q)
    assertEquals(29, rs.collect().length)
    // fire again
    rs = ownerContext.sql(q)
    assertEquals(29, rs.collect().length)

    // fire again
    rs = snc2.sql(q)
    assertEquals(29, rs.collect().length)


    // now create a policy
    ownerContext.sql(s"create policy testPolicy1 on  " +
        s"$tableName for select to current_user using id < 30")

    rs = ownerContext.sql(q)
    assertEquals(29, rs.collect().length)

    rs = snc2.sql(q)
    assertEquals(0, rs.collect().length)
    ownerContext.sql("drop policy testPolicy1")

  }

  test("old query plan invalidation on enabling rls on column table using snappy context") {
    this.testQueryPlanInvalidationOnRLSEnbaling(colTableName)
  }

  test("old query plan invalidation on enabling rls on row table using snappy context") {
    this.testQueryPlanInvalidationOnRLSEnbaling(rowTableName)
  }

  private def testQueryPlanInvalidationOnRLSEnbaling(tableName: String): Unit = {
    // first disable RLS
    ownerContext.sql(s"alter table $tableName disable row level security")
    // now create a policy
    ownerContext.sql(s"create policy testPolicy1 on  " +
        s"$tableName for select to current_user using id < 30")

    val snc2 = snc.newSession()
    snc2.snappySession.conf.set(Attribute.USERNAME_ATTR, "UserX")
    val q = s"select * from $tableName where id > 70"

    var rs = snc2.sql(q)
    assertEquals(29, rs.collect().length)
    // fire again
    rs = snc2.sql(q)
    assertEquals(29, rs.collect().length)
    // fire again
    rs = ownerContext.sql(q)
    assertEquals(29, rs.collect().length)

    rs = ownerContext.sql(q)
    assertEquals(29, rs.collect().length)

    // fire again
    rs = snc2.sql(q)
    assertEquals(29, rs.collect().length)

    // Now enable RLS

    ownerContext.sql(s"alter table $tableName enable row level security")


    rs = ownerContext.sql(q)
    assertEquals(29, rs.collect().length)

    rs = snc2.sql(q)
    assertEquals(0, rs.collect().length)
    ownerContext.sql("drop policy testPolicy1")

  }

  test("test bug causing recursion with query having filter using col table - ENT-40") {
    this.testRecursionBug(colTableName)
  }

  test("test bug causing recursion with query having filter using row table - ENT-40") {
    this.testRecursionBug(rowTableName)
  }


  test("test policy filter with subquery for row table") {
    this.whereClauseWithExistsCondition(rowTableName)
  }

  test("test policy filter with subquery for col table") {
    this.whereClauseWithExistsCondition(colTableName)
  }

  private def whereClauseWithExistsCondition(tableName: String): Unit = {
    val mappingTable = "mapping"
    ownerContext.sql(s"CREATE TABLE $mappingTable (username String, hisid Int) " +
        s" USING row ")
    val seq = Seq("USERX" -> 4, "USERX" -> 5, "USERX" -> 6, "USERY" -> 7,
      "USERY" -> 8, "USERY" -> 9)
    val rdd = sc.parallelize(seq)

    val dataDF = ownerContext.createDataFrame(rdd)

    dataDF.write.insertInto(mappingTable)

    ownerContext.sql(s"create policy testPolicy1 on  " +
        s"$tableName for select to current_user using " +
        s"exists( select 1 from $tableOwner.$mappingTable " +
        s" where username = current_user() and id = hisid)")

    val snc2 = snc.newSession()
    snc2.snappySession.conf.set(Attribute.USERNAME_ATTR, "UserX")
    val q1 = s"select * from $tableName "
    var rs = snc2.sql(q1).collect()
    assertEquals(3, rs.length)
    var idResults = rs.map(_.getInt(1))
    assertTrue(idResults.contains(4))
    assertTrue(idResults.contains(5))
    assertTrue(idResults.contains(6))

    // fire the query but use table alias and a filter
    val q2 = s"select * from $tableName x where x.id  < 6 "
    rs = snc2.sql(q2).collect()
    assertEquals(2, rs.length)
    idResults = rs.map(_.getInt(1))
    assertTrue(idResults.contains(4))
    assertTrue(idResults.contains(5))


    ownerContext.sql("drop policy testPolicy1")
    ownerContext.sql(s"drop table $mappingTable")

  }

  test("test policy filter with subquery for row table with row table joined to itself") {
    this.tableWithPolicyJoinedToItself(rowTableName)
  }

  test("test policy filter with subquery for col table with col table joined to itself") {
    this.tableWithPolicyJoinedToItself(colTableName)
  }

  test("test policy not applied for update | delete on row table - SNAP-2576") {
    this.updateOrDeleteOntableWithPolicy("row")
  }

  test("test policy not applied for update | delete on column table - SNAP-2576") {
    this.updateOrDeleteOntableWithPolicy("column")
  }

  private def updateOrDeleteOntableWithPolicy(tableType: String): Unit = {

    ownerContext.sql(s"CREATE TABLE temp (username String, id Int) " +
        s" USING $tableType ")
    val seq = Seq("USERX" -> 4, "USERX" -> 5, "USERX" -> 6, "USERY" -> 7,
      "USERY" -> 8, "USERY" -> 9)
    val rdd = sc.parallelize(seq)

    val dataDF = ownerContext.createDataFrame(rdd)

    dataDF.write.insertInto("temp")

    ownerContext.sql(s"create policy testPolicy1 on  " +
        s" temp for select to current_user using " +
        s" id < 0")

    ownerContext.sql("alter table temp enable row level security")

    val snc2 = snc.newSession()
    snc2.snappySession.conf.set(Attribute.USERNAME_ATTR, "UserX")
    val q1 = s"select * from $tableOwner.temp"
    var rs = snc2.sql(q1).collect()
    assertEquals(0, rs.length)

    snc2.sql(s"update $tableOwner.temp set username = 'USERZ' where username = 'USERX'")

    rs = ownerContext.sql(s"select * from temp where username = 'USERZ'").collect()
    assertEquals(3, rs.length)

    snc2.sql(s"delete from $tableOwner.temp  where username = 'USERZ'")

    rs = ownerContext.sql(s"select * from temp where username = 'USERZ'").collect()
    assertEquals(0, rs.length)

    ownerContext.sql("drop policy testPolicy1")
    ownerContext.sql(s"drop table temp")
  }

  private def tableWithPolicyJoinedToItself(tableName: String): Unit = {
    val mappingTable = "mapping"
    ownerContext.sql(s"CREATE TABLE $mappingTable (username String, hisid Int) " +
        s" USING row ")
    val seq = Seq("USERX" -> 4, "USERX" -> 5, "USERX" -> 6, "USERY" -> 7,
      "USERY" -> 8, "USERY" -> 9)
    val rdd = sc.parallelize(seq)

    val dataDF = ownerContext.createDataFrame(rdd)

    dataDF.write.insertInto(mappingTable)

    ownerContext.sql(s"create policy testPolicy1 on  " +
        s"$tableName for select to current_user using " +
        s"exists( select 1 from $tableOwner.$mappingTable " +
        s" where username = current_user() and id = hisid)")

    val snc2 = snc.newSession()
    snc2.snappySession.conf.set(Attribute.USERNAME_ATTR, "UserX")
    val q1 = s"select * from $tableName tab1 , $tableName tab2 " +
        s"where tab1.id < 6 and tab2.id < 6 "
    val rs = snc2.sql(q1).collect()
    assertEquals(4, rs.length)
    val idResults = rs.map(x => x.getInt(1) -> x.get(3))
    assertTrue(idResults.contains((4, 4)))
    assertTrue(idResults.contains((4, 5)))
    assertTrue(idResults.contains((5, 4)))
    assertTrue(idResults.contains((5, 5)))
    ownerContext.sql("drop policy testPolicy1")
    ownerContext.sql(s"drop table $mappingTable")
  }

  private def testRecursionBug(tableName: String): Unit = {
    ownerContext.sql(s"create policy testPolicy1 on  " +
        s"$tableName for select to userX using id < 30 and name = 'name_1'")
    val snc2 = snc.newSession()
    snc2.snappySession.conf.set(Attribute.USERNAME_ATTR, "UserX")
    var q = s"select * from $tableName where id < 20 and name = 'name_1'"

    var rs = snc2.sql(q).collect()
    assertEquals(1, rs.length)
    // now create another policy
    ownerContext.sql(s"create policy testPolicy2 on  " +
        s"$tableName for select to userX using id < 20 and name = 'name_2'")
    rs = snc2.sql(q).collect()
    assertEquals(0, rs.length)

    ownerContext.sql(s"create policy testPolicy3 on  " +
        s"$tableName for select to userX using id < 10 and name = 'name_4'")

    ownerContext.sql(s"create policy testPolicy4 on  " +
        s"$tableName for select to userX using id < 5 and name = 'name_5'")

    q = s"select * from $tableName where id < 20 and name = 'name_1' and id > 10 " +
        s"and name = 'name7'"

    rs = snc2.sql(q).collect()
    assertEquals(0, rs.length)
    ownerContext.sql("drop policy testPolicy1")
    ownerContext.sql("drop policy testPolicy2")
    ownerContext.sql("drop policy testPolicy3")
    ownerContext.sql("drop policy testPolicy4")

  }

  private def testMultiplePolicy(tableName: String) {
    ownerContext.sql(s"create policy testPolicy1 on  " +
        s"$tableName for select to current_user using id > 10")
    ownerContext.sql(s"create policy testPolicy2 on  " +
        s"$tableName for select to current_user using id < 20")
    var rs = ownerContext.sql(s"select * from $tableName").collect()
    assertEquals(numElements, rs.length)

    val snc2 = snc.newSession()
    snc2.snappySession.conf.set(Attribute.USERNAME_ATTR, "UserX")

    rs = snc2.sql(s"select * from $tableName").collect()
    assertEquals(9, rs.length)
    ownerContext.sql("drop policy testPolicy1")
    ownerContext.sql("drop policy testPolicy2")
  }

  test("Policy creation & dropping allowed by all users if security is disabled") {
    val snc2 = snc.newSession()
    snc2.snappySession.conf.set(Attribute.USERNAME_ATTR, "UserX")
    snc2.sql(s"create policy testPolicy2 on  " +
        s"$colTableName for select to current_user using id > 10")
    snc2.sql("drop policy testPolicy2")

  }

  test("Drop table with policies") {
    val seq2 = for (i <- 0 until numElements) yield {
      (s"name_$i", i)
    }
    val rdd2 = sc.parallelize(seq2)
//    ownerContext = snc.newSession()
//    ownerContext.snappySession.conf.set(Attribute.USERNAME_ATTR, tableOwner)

    val dataDF2 = ownerContext.createDataFrame(rdd2)

    val colTableName2: String = s"$tableOwner.ColumnTable2"
    val rowTableName2: String = s"$tableOwner.RowTable2"
    val colTableName3: String = s"$tableOwner.ColumnTable3"

    ownerContext.sql(s"CREATE TABLE $colTableName2 (name String, id Int) " +
        s" USING column ")
    ownerContext.sql(s"CREATE TABLE $rowTableName2 (name String, id Int) " +
        s" USING row ")
    ownerContext.sql(s"CREATE TABLE $colTableName3 (name String, id Int) " +
        s" USING column ")

    dataDF2.write.insertInto(colTableName2)
    dataDF2.write.insertInto(rowTableName2)
    dataDF2.write.insertInto(colTableName3)
    ownerContext.sql(s"alter table $colTableName2 enable row level security")
    ownerContext.sql(s"alter table $rowTableName2 enable row level security")
    ownerContext.sql(s"alter table $colTableName3 enable row level security")

    ownerContext.sql(s"create policy testPolicy1_for_ColumnTable3 on  " +
        s"$colTableName3 for select to current_user using id > 11")
    ownerContext.sql(s"create policy testPolicy2_for_ColumnTable3 on  " +
        s"$colTableName3 for select to current_user using id < 22")

    testDropTable(colTableName2)
    testDropTable(rowTableName2)

    // colTableName3 was not dropped, so policies should exist
    assert(checkIfPoliciesOnTableExist(colTableName3))

    testDropTable(colTableName3)
  }

  private def testDropTable(tableName: String) {
    ownerContext.sql(s"create policy testPolicy11 on  " +
        s"$tableName for select to current_user using id > 11")
    ownerContext.sql(s"create policy testPolicy22 on  " +
        s"$tableName for select to current_user using id < 22")
    ownerContext.sql(s"drop table $tableName")
    assert(!checkIfPoliciesOnTableExist(tableName), s"Policy for $tableName should not be present")
  }

  // return true if a policy exists for a table else false
  private def checkIfPoliciesOnTableExist(tableName: String): Boolean = {
    val policies = Misc.getMemStore.getExternalCatalog.getPolicies
    val it = policies.listIterator()
    while (it.hasNext) {
      val p = it.next()
      //      println("Actual tablename:" + tableName + ", tableName in policy:" + p.tableName)
      if ((p.schemaName + "." + p.tableName).equalsIgnoreCase(tableName)) {
        return true
      }
    }
    false
  }
}
