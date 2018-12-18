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

package org.apache.spark.sql.internal;

import java.lang.reflect.InvocationTargetException;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.SnappyContext$;
import org.apache.spark.sql.SnappySession;
import org.apache.spark.sql.SnappySession$;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.SnappySessionState;

/**
 * Base class for SnappySession that allows having the SessionState and SharedState
 * as variables that can be changed when required (for hive support).
 */
public abstract class SessionBase extends SparkSession {

  private static final long serialVersionUID = 7013637782126648003L;

  private transient volatile SessionState snappySessionState;

  private transient volatile SharedState snappySharedState;

  public SessionBase(SparkContext sc) {
    super(sc);
  }

  /**
   * State isolated across sessions, including SQL configurations, temporary tables, registered
   * functions, and everything else that accepts a {@link org.apache.spark.sql.internal.SQLConf}.
   */
  @Override
  public SessionState sessionState() {
    SessionState sessionState = this.snappySessionState;
    if (sessionState != null) return sessionState;
    synchronized (this) {
      sessionState = this.snappySessionState;
      if (sessionState != null) return sessionState;
      scala.Option<Class<?>> sessionStateClass = SnappySession$.MODULE$.aqpSessionStateClass();
      if (sessionStateClass.isEmpty()) {
        sessionState = this.snappySessionState = new SnappySessionState((SnappySession)this);
      } else {
        Class<?> aqpClass = sessionStateClass.get();
        try {
          sessionState = this.snappySessionState = (SessionState)aqpClass.getConstructor(
              SnappySession.class).newInstance((SnappySession)this);
        } catch (InvocationTargetException e) {
          throw new RuntimeException(e.getCause());
        } catch (RuntimeException e) {
          throw e;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      return sessionState;
    }
  }

  public void setSessionState(SessionState sessionState) {
    this.snappySessionState = sessionState;
  }

  /**
   * State shared across sessions, including the {@link SparkContext}, cached data, listener,
   * and a catalog that interacts with external systems.
   */
  @Override
  public SharedState sharedState() {
    SharedState sharedState = this.snappySharedState;
    if (sharedState != null) return sharedState;
    synchronized (this) {
      sharedState = this.snappySharedState;
      return sharedState != null ? sharedState
          : (this.snappySharedState = SnappyContext$.MODULE$.sharedState(sparkContext()));
    }
  }

  public void setSharedState(SharedState sharedState) {
    this.snappySharedState = sharedState;
  }
}
