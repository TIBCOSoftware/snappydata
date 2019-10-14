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
package org.apache.spark.ui

import java.util
import java.util.Properties
import javax.security.auth.Subject
import javax.servlet.ServletRequest
import javax.servlet.http.HttpServletRequest

import scala.collection.JavaConverters._

import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.engine.{GfxdConstants, Misc}
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager
import org.eclipse.jetty.security.DefaultUserIdentity
import org.eclipse.jetty.security.authentication.BasicAuthenticator
import org.eclipse.jetty.server.{Request, UserIdentity}
import templates.security.UsernamePrincipal

import org.apache.spark.Logging

class SnappyBasicAuthenticator extends BasicAuthenticator with Logging {

  override def login(username: String, password: Any, request: ServletRequest): UserIdentity = {

    val props = new Properties()
    props.setProperty(Attribute.USERNAME_ATTR, username)
    props.setProperty(Attribute.PASSWORD_ATTR, password.toString)

    val memStore = Misc.getMemStoreBooting
    val result = memStore.getDatabase.getAuthenticationService.authenticate(
      memStore.getDatabaseName, props)

    if (result != null) {
      val msg = s"ACCESS DENIED, user [$username]. $result"
      if (GemFireXDUtils.TraceAuthentication) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_AUTHENTICATION, msg)
      }
      null
    } else if (Misc.getGemFireCache.isSnappyRecoveryMode &&
        !username.equals(memStore.getBootProperty(Attribute.USERNAME_ATTR))) {
      val msg = s"ACCESS DENIED, user [$username]. Only admin-user is allowed in recovery mode."
      if (GemFireXDUtils.TraceAuthentication) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_AUTHENTICATION, msg)
      }
      null
    } else {
      val principal = new UsernamePrincipal(username)
      val response = request match {
        case r: Request => r.getResponse
        case _ => null
      }
      this.renewSession(request.asInstanceOf[HttpServletRequest], response)

      new DefaultUserIdentity(new Subject(false, Set(principal).asJava, new util.HashSet(),
        new util.HashSet()), principal, JettyUtils.snappyDataRoles)
    }
  }
}