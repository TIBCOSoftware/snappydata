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
import com.pivotal.gemfirexd.Property.{AUTH_LDAP_SEARCH_BASE, AUTH_LDAP_SERVER}
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore
import com.pivotal.gemfirexd.internal.iapi.reference.Property
import com.pivotal.gemfirexd.security.{LdapTestServer, SecurityTestUtils}
import io.snappydata.{Constant, SnappyFunSuite}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.{Logging, SparkConf}

abstract class PolicyTestBase extends SnappyFunSuite
    with Logging
    with BeforeAndAfter
    with BeforeAndAfterAll {

  protected val sysUser = "gemfire10"

  override def beforeAll(): Unit = {
    super.beforeAll()
    stopAll()

    System.setProperty(Property.SNAPPY_ENABLE_RLS, "true")
    GemFireStore.ALLOW_RLS_WITHOUT_SECURITY = true
  }

  protected def newLDAPSparkConf(addOn: (SparkConf) => SparkConf): SparkConf = {
    val ldapProperties = SecurityTestUtils.startLdapServerAndGetBootProperties(0, 0, sysUser,
      getClass.getResource("/auth.ldif").getPath)
    for (k <- List(Attribute.AUTH_PROVIDER, AUTH_LDAP_SERVER, AUTH_LDAP_SEARCH_BASE)) {
      System.setProperty(k, ldapProperties.getProperty(k))
    }
    System.setProperty(Constant.STORE_PROPERTY_PREFIX + Attribute.USERNAME_ATTR, sysUser)
    System.setProperty(Constant.STORE_PROPERTY_PREFIX + Attribute.PASSWORD_ATTR, sysUser)
    val conf = new org.apache.spark.SparkConf()
        .setAppName("PolicyTest")
        .setMaster("local[4]")
        .set("spark.sql.crossJoin.enabled", "true")
        .set(Attribute.AUTH_PROVIDER, ldapProperties.getProperty(Attribute.AUTH_PROVIDER))
        .set(Constant.STORE_PROPERTY_PREFIX + Attribute.USERNAME_ATTR, sysUser)
        .set(Constant.STORE_PROPERTY_PREFIX + Attribute.PASSWORD_ATTR, sysUser)

    if (addOn != null) {
      addOn(conf)
    } else {
      conf
    }
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
      stopAll()

      val ldapServer = LdapTestServer.getInstance()
      if (ldapServer.isServerStarted) {
        ldapServer.stopService()
      }
    } finally {
      for (k <- List(Attribute.AUTH_PROVIDER, AUTH_LDAP_SERVER, AUTH_LDAP_SEARCH_BASE)) {
        System.clearProperty(k)
        System.clearProperty("gemfirexd." + k)
        System.clearProperty(Constant.STORE_PROPERTY_PREFIX + k)
      }
      System.clearProperty(Constant.STORE_PROPERTY_PREFIX + Attribute.USERNAME_ATTR)
      System.clearProperty(Constant.STORE_PROPERTY_PREFIX + Attribute.PASSWORD_ATTR)
      System.setProperty("gemfirexd.authentication.required", "false")
      GemFireStore.ALLOW_RLS_WITHOUT_SECURITY = false
      System.clearProperty(Property.SNAPPY_ENABLE_RLS)
    }
  }
}
