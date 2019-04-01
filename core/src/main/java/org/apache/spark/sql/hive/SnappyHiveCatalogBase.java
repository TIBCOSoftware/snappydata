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
package org.apache.spark.sql.hive;

import javax.annotation.concurrent.GuardedBy;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.hive.client.HiveClient;

/**
 * This intermediate class is in java to enable overriding the "val client" with a method
 * that will return a custom hive client object. This enables the actual client to be
 * a "var" which is used to handle retries in case of transient disconnect exceptions.
 */
public abstract class SnappyHiveCatalogBase extends HiveExternalCatalog {

  @GuardedBy("this")
  protected HiveClient hiveClient;

  protected SnappyHiveCatalogBase(SparkConf conf, Configuration hadoopConf) {
    super(conf, hadoopConf);
    // initialize with super's hive client but allow this to be recreated
    // in case of transient disconnect exceptions
    this.hiveClient = super.client();
  }

  @Override
  public final HiveClient client() {
    return this.hiveClient;
  }
}
