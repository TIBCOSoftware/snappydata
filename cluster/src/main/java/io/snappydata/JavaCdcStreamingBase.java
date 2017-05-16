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
package io.snappydata;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SnappySession;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.StreamingQuery;

public abstract class JavaCdcStreamingBase {

  private final String extraClassPath;
  private final ArrayList<String> tables = new ArrayList<>();
  private final Map<String, String> extraConfProps;

  public JavaCdcStreamingBase(String[] args, Map<String, String> extraConfProps) throws Exception {
    if (args.length < 6) {
      throw new Exception("Insufficient arguments. <driver> <url> <user> <password> <tables> " +
          "<snappyurl> must be provided");
    }

    this.initProps = new Properties();
    for (String s : args) {
      String[] arg = s.split("=");
      this.initProps.setProperty(arg[0], arg[1]);
    }

    this.extraClassPath = initProps.getProperty("extraClassPath");

    Collections.addAll(this.tables, initProps.getProperty("tables").split(","));

    this.extraConfProps = extraConfProps;
  }

  /**
   * Atleast set "spark.master" or other cluster manager configuration.
   * If not set, default is "local[4]" meaning driver/executors running within a single jvm with
   * four threads.
   *
   * @param conf SparkConf where properties is to be set.
   * @return may or may not return the same conf object passed in as argument.
   */
  protected abstract SparkConf extraConf(SparkConf conf);

  protected SnappySession connect() throws ClassNotFoundException {
    Class.forName(initProps.getProperty("driver"));
    final SparkConf conf = new SparkConf().
        setAppName(this.getClass().getName());
    if (extraClassPath != null) {
      conf.setJars(extraClassPath.split(File.pathSeparator))
          .set("spark.driver.extraClassPath", extraClassPath)
          .set("spark.executor.extraClassPath", extraClassPath);
    }

    initProps.forEach((k, v) -> {
      String keyStr = k.toString();
      if (keyStr.startsWith("snappydata.") || keyStr.startsWith("spark.")) {
        conf.set(keyStr, v.toString());
      }
      // special handling until case of this property is changed to all lower case.
      else if (keyStr.toLowerCase().startsWith("snappydata.Cluster.URL".toLowerCase())) {
        conf.set("snappydata.Cluster.URL", v.toString());
      }
    });

    //    set("snappydata.store.locators", "localhost[10334]");

    SparkConf newConf = extraConf(conf);
    newConf.setIfMissing("spark.master", "local[12]");
    snappySpark = new SnappySession(SparkSession.
        builder().
        config(newConf).
        getOrCreate().sparkContext());
    return snappySpark;
  }

  protected abstract StreamingQuery getStreamWriter(String tableName,
      Dataset<Row> reader) throws Exception;

  protected void startJob() throws Exception {
    ArrayList<StreamingQuery> activeQueries = new ArrayList<>(tables.size());
    for (String tab : tables) {
      DataStreamReader conf = snappySpark.readStream()
          .format("jdbcStream");

      initProps.forEach((k, v) -> {
        conf.option(k.toString(), v.toString());
      });
      conf.option("dbtable", tab);

      for (Map.Entry<String, String> e : extraConfProps.entrySet()) {
        conf.option(e.getKey(), e.getValue());
      }
      Dataset<Row> reader = conf.load();
      StreamingQuery q = getStreamWriter(tab, reader);
      activeQueries.add(q);
    }

    for (StreamingQuery q : activeQueries) {
      q.awaitTermination();
    }
  }

  private final Properties initProps;
  private SnappySession snappySpark;

}
