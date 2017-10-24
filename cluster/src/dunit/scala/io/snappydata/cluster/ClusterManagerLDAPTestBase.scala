/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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
package io.snappydata.cluster

import java.util.Properties

import scala.language.postfixOps

import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.security.{LdapTestServer, SecurityTestUtils}

/**
 * Base class for start and stop of LDAP Server
 */
object ClusterManagerLDAPTestBase{
  val securityProperties: Properties = new Properties()
}

abstract class ClusterManagerLDAPTestBase(s: String, val adminUser: String = "gemfire10")
    extends ClusterManagerTestBase(s) with Serializable {

  override def beforeClass(): Unit = {
    val ldapProperties = SecurityTestUtils.startLdapServerAndGetBootProperties(0, 0, adminUser,
      getClass.getResource("/auth.ldif").getPath)
    setSecurityProps(ldapProperties)
    super.beforeClass()
    SplitClusterDUnitSecurityTest.bootExistingAuthModule(ldapProperties)
  }

  override def afterClass(): Unit = {
    try {
      super.afterClass()
    } finally {
      val ldapServer = LdapTestServer.getInstance()
      if (ldapServer.isServerStarted) {
        ldapServer.stopService()
      }
      ClusterManagerLDAPTestBase.securityProperties.clear()
    }
  }

  override def setUp(): Unit = {
    ClusterManagerLDAPTestBase.securityProperties.keySet().toArray.foreach(k =>
      bootProps.put(k, ClusterManagerLDAPTestBase.securityProperties.get(k)))
    super.setUp()
  }

  def setSecurityProps(ldapProperties: Properties): Unit = {
    import com.pivotal.gemfirexd.Property.{AUTH_LDAP_SERVER, AUTH_LDAP_SEARCH_BASE}
    for (k <- List(Attribute.AUTH_PROVIDER, AUTH_LDAP_SERVER, AUTH_LDAP_SEARCH_BASE)) {
      System.setProperty(k, ldapProperties.getProperty(k))
    }
    for (k <- List(Attribute.AUTH_PROVIDER, AUTH_LDAP_SERVER, AUTH_LDAP_SEARCH_BASE,
      Attribute.USERNAME_ATTR, Attribute.PASSWORD_ATTR)) {
      locatorNetProps.setProperty(k, ldapProperties.getProperty(k))
      bootProps.setProperty(k, ldapProperties.getProperty(k))
      ClusterManagerLDAPTestBase.securityProperties.setProperty(k, ldapProperties.getProperty(k))
    }
  }
}


