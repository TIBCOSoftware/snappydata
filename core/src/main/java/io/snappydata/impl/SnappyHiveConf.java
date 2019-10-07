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
package io.snappydata.impl;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

/**
 * Extension of HiveConf to access few protected variables.
 */
public class SnappyHiveConf extends HiveConf {

  private static Field loadDefaultsField;
  private static Field resourcesField;
  private static Method getConfVarInputStreamMethod;
  private static Method setupSQLStdAuthWhiteListMethod;
  private static Method setupRestrictListMethod;

  static {
    try {
      loadDefaultsField = Configuration.class.getDeclaredField("loadDefaults");
      loadDefaultsField.setAccessible(true);
      resourcesField = Configuration.class.getDeclaredField("resources");
      resourcesField.setAccessible(true);
      getConfVarInputStreamMethod = HiveConf.class.getDeclaredMethod(
          "getConfVarInputStream");
      getConfVarInputStreamMethod.setAccessible(true);
    } catch (NoSuchFieldException | NoSuchMethodException e) {
      // disable resources reset calls
      loadDefaultsField = null;
      resourcesField = null;
      getConfVarInputStreamMethod = null;
    }
    try {
      setupSQLStdAuthWhiteListMethod = HiveConf.class.getDeclaredMethod(
          "setupSQLStdAuthWhiteList");
      setupSQLStdAuthWhiteListMethod.setAccessible(true);
      setupRestrictListMethod = HiveConf.class.getDeclaredMethod(
          "setupRestrictList");
      setupRestrictListMethod.setAccessible(true);
    } catch (NoSuchMethodException e) {
      // disable setter calls
      setupSQLStdAuthWhiteListMethod = null;
      setupRestrictListMethod = null;
    }
  }

  public SnappyHiveConf() {
    try {
      // create a blank HiveConf ignoring configuration from any hive-site.xml
      clear();

      // clear unwanted resources
      if (loadDefaultsField != null) {
        loadDefaultsField.setBoolean(this, false);
        ArrayList<?> resources = (ArrayList<?>)resourcesField.get(this);
        resources.clear();
        addResource((InputStream)getConfVarInputStreamMethod.invoke(this));
      }

      for (HiveConf.ConfVars confVar : HiveConf.ConfVars.values()) {
        String defaultValue = confVar.getDefaultValue();
        if (defaultValue != null) {
          set(confVar.varname, defaultValue);
        }
      }

      // reset hive jar configuration
      this.hiveJar = "";
      this.auxJars = "";

      // reset restriction lists
      if (setupSQLStdAuthWhiteListMethod != null) {
        setupSQLStdAuthWhiteListMethod.invoke(this);
        setupRestrictListMethod.invoke(this);
      }
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }
}
