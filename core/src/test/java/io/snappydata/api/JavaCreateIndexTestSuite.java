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
package io.snappydata.api;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SnappyContext;
import org.junit.Before;
import org.junit.Test;


public class JavaCreateIndexTestSuite implements Serializable {

  private static final String master = "local[2]";
  private static final String appName = "SampleApp";
  private static final SparkConf conf = new SparkConf().setMaster(master).setAppName(appName);
  private static final JavaSparkContext context = new JavaSparkContext(conf);
  private static final SnappyContext snc = SnappyContext.apply(context);

  @Before
  public void setUp() {

    List<DummyBeanClass> dummyList = new ArrayList<DummyBeanClass>();
    for (int i = 0; i < 2; i++) {
      DummyBeanClass object = new DummyBeanClass();
      object.setCol2("" + i);
      object.setCol1(i);
      dummyList.add(object);
    }

    JavaRDD<DummyBeanClass> rdd = context.parallelize(dummyList);
    Dataset<Row> df = snc.createDataFrame(rdd, DummyBeanClass.class);
    Map<String, String> properties = new HashMap<>();

    properties.put("PARTITION_BY", "col2");
    Dataset<Row> tableDf = snc.createTable("table2", "row", df.schema(), properties, true);
    tableDf.write().format("column").mode(SaveMode.Append).saveAsTable("table1");
  }

  @Test
  public void createIndexWithAscendingDirection() {
    Map<String, Boolean> indexColumns = new HashMap<>();
    indexColumns.put("col1", true);

    snc.createIndex("test1", "table1", indexColumns, new HashMap<String, String>());

    //Test will fail if drop index which is not created by above api fail
    snc.dropIndex("test1", false);
    snc.dropTable("table1", true);

  }


  @Test
  public void createIndexWithDescendingDirection() {
    Map<String, Boolean> indexColumns = new HashMap<>();
    indexColumns.put("col1", false);

    snc.createIndex("test1", "table1", indexColumns, new HashMap<String, String>());
    snc.dropIndex("test1", false);
    snc.dropTable("table1", true);

  }

  @Test
  public void createIndexWithNoneDirection() {
    Map<String, Boolean> indexColumns = new HashMap<>();
    indexColumns.put("col1", null);
    Map<String, String> options = new HashMap<String, String>();
    options.put("index_type", "global hash");
    snc.createIndex("test1", "table1", indexColumns, options);

    snc.dropIndex("test1", false);
    snc.dropTable("table1", true);
  }


  @Test
  public void createIndexWithNoneDirectionAndOptions() {
    Map<String, Boolean> indexColumns = new HashMap<>();
    indexColumns.put("col1", null);

    Map<String, String> options = new HashMap<String, String>();
    options.put("index_type", "unique");

    snc.createIndex("test1", "table1", indexColumns, options);

    snc.dropIndex("test1", false);
  }


}

class DummyBeanClass implements Serializable {

  Integer col1;
  String col2;

  public Integer getCol1() {
    return col1;
  }

  public String getCol2() {
    return col2;
  }

  public void setCol1(Integer col1) {
    this.col1 = col1;
  }

  public void setCol2(String col2) {
    this.col2 = col2;
  }
}
