/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Changes for SnappyData data platform.
 *
 * Portions Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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
package io.snappydata.test.dunit.logging.log4j;

import java.io.File;

/**
 * Utility methods for finding the Log4J 2 configuration file.
 *
 * TODO This class is copied from GEODE source but put into snappy-dunits.
 * TODO The current package is renamed from com.gemstone.gemfire.internal.logging.log4j to avoid package sealing issue.
 *
 * @author Kirk Lund
 */
public class ConfigLocator {

  static final String PREFIX = "log4j12";

  static final String SUFFIX_TEST_YAML = "-test.yaml";
  static final String SUFFIX_TEST_YML = "-test.yml";
  static final String SUFFIX_TEST_JSON = "-test.json";
  static final String SUFFIX_TEST_JSN = "-test.jsn";
  static final String SUFFIX_TEST_XML = "-test.xml";
  static final String SUFFIX_YAML = ".yaml";
  static final String SUFFIX_YML = ".yml";
  static final String SUFFIX_JSON = ".json";
  static final String SUFFIX_JSN = ".jsn";
  static final String SUFFIX_XML = ".xml";
  
  /** Ordered as specified on http://logging.apache.org/log4j/2.x/manual/configuration.html */
  static final String[] SUFFIXES = new String[] { SUFFIX_TEST_YAML, SUFFIX_TEST_YML, SUFFIX_TEST_JSON, SUFFIX_TEST_JSN, SUFFIX_TEST_XML, SUFFIX_YAML, SUFFIX_YML, SUFFIX_JSON, SUFFIX_JSN, SUFFIX_XML };
  
  /**
   * Finds a Log4j configuration file in the current working directory.  The 
   * names of the files to look for are the same as those that Log4j would look 
   * for on the classpath.
   * 
   * @return configuration file or null if not found.
   */
  public static File findConfigInWorkingDirectory() {    
    for (final String suffix : SUFFIXES) {
      final File configFile = new File(System.getProperty("user.dir"), PREFIX + suffix);
      if (configFile.isFile()) {
        return configFile;
      }
    }

    return null;
  }
}
