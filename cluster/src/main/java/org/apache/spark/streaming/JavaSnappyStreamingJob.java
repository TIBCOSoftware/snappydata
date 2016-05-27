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
import org.apache.spark.sql.JSparkJobValidation;
import org.apache.spark.sql.JavaJobValidate;
import org.apache.spark.sql.streaming.SnappyStreamingJob;
import org.apache.spark.streaming.api.java.JavaSnappyStreamingContext;
import spark.jobserver.SparkJobValidation;

public abstract class JavaSnappyStreamingJob implements SnappyStreamingJob {

  abstract public  Object runJavaJob(JavaSnappyStreamingContext snc, Config jobConfig);

  abstract public JSparkJobValidation isValidJob(JavaSnappyStreamingContext snc,
      Config jobConfig);

  @Override
  public Object runJob(Object sc, Config jobConfig) {
    return runJavaJob(new JavaSnappyStreamingContext((SnappyStreamingContext)sc), jobConfig);
  }

  @Override
  public SparkJobValidation validate(Object sc, Config config) {
    JSparkJobValidation  status =
        isValidJob(new JavaSnappyStreamingContext((SnappyStreamingContext)sc), config);
    return JavaJobValidate.validate(status);
  }
}
