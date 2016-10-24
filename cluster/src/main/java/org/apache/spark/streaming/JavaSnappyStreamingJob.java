/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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
package org.apache.spark.streaming;


import com.typesafe.config.Config;
import org.apache.spark.sql.SnappyJobValidate;
import org.apache.spark.sql.SnappyJobValidation;
import org.apache.spark.streaming.api.java.JavaSnappyStreamingContext;

import org.apache.spark.util.SnappyUtils;
import spark.jobserver.SparkJobBase;
import spark.jobserver.SparkJobValidation;

public abstract class JavaSnappyStreamingJob implements SparkJobBase {

  abstract public Object runSnappyJob(JavaSnappyStreamingContext snc, Config jobConfig);

  abstract public SnappyJobValidation isValidJob(JavaSnappyStreamingContext snc,
      Config jobConfig);

  @Override
  final public SparkJobValidation validate(Object sc, Config config) {
    ClassLoader parentLoader = org.apache.spark.util.Utils.getContextOrSparkClassLoader();
    ClassLoader currentLoader = SnappyUtils.getSnappyStoreContextLoader(parentLoader);
    Thread.currentThread().setContextClassLoader(currentLoader);
    return SnappyJobValidate.validate(isValidJob(new JavaSnappyStreamingContext((SnappyStreamingContext)sc), config));
  }

  @Override
  final public Object runJob(Object sc, Config jobConfig) {
    JavaSnappyStreamingContext context = new JavaSnappyStreamingContext((SnappyStreamingContext)sc);
    try {
      return runSnappyJob(context, jobConfig);
    } finally {
      SnappyUtils.removeJobJar(context.snsc().sparkContext());
    }
  }

}