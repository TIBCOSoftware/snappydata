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
package io.snappydata.api;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState;
import io.snappydata.Property;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SnappyContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


public class JavaCreateIndexTestSuite implements Serializable {

  private static final String master = "local[2]";
  private static final String appName = "SampleApp";
  private static final SparkConf conf = new SparkConf().setMaster(master).setAppName(appName);
  private static final JavaSparkContext context = new JavaSparkContext(conf);
  private static final SnappyContext snc = SnappyContext.apply(context);

  @BeforeClass
  public static void beforeAll() {
    snc.setConf(Property.EnableExperimentalFeatures().name(), "true");
    List<DummyBeanClass> dummyList = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      DummyBeanClass object = new DummyBeanClass();
      object.setCol2("" + i);
      object.setCol1(i);
      dummyList.add(object);
    }

    @SuppressWarnings("RedundantCast")
    JavaRDD<DummyBeanClass> rdd = (JavaRDD<DummyBeanClass>)context.parallelize(dummyList);
    Dataset<Row> df = snc.createDataFrame(rdd, DummyBeanClass.class);
    Map<String, String> properties = new HashMap<>();

    properties.put("PARTITION_BY", "col2");
    Dataset<Row> tableDf = snc.createTable("table2", "row", df.schema(), properties, true);
    tableDf.write().format("column").mode(SaveMode.Append).saveAsTable("table1");
  }

  @AfterClass
  public static void afterAll() {
    snc.dropTable("table1", false);
    snc.dropTable("table2", false);
  }

  @Test
  public void createIndexWithAscendingDirection() {
    snc.createIndex("test1", "table1", Collections.singletonList("col1"),
        Collections.singletonList(true), Collections.emptyMap());
    // Test will fail if drop index which is not created by above api fail
    snc.dropIndex("test1", false);

    snc.createIndex("test2", "table2", Collections.singletonList("col1"),
        Collections.singletonList(true), Collections.emptyMap());
    snc.dropIndex("test2", false);
  }

  @Test
  public void createIndexWithDescendingDirection() {
    // should fail for column tables
    try {
      snc.createIndex("test1", "table1", Collections.singletonList("col1"),
          Collections.singletonList(false), Collections.emptyMap());
      Assert.fail("expected to fail");
    } catch (UnsupportedOperationException expected) {
    }

    snc.createIndex("test2", "table2", Collections.singletonList("col1"),
        Collections.singletonList(false), Collections.emptyMap());
    snc.dropIndex("test2", false);
  }

  @Test
  public void createIndexWithNoneDirection() {
    snc.createIndex("test1", "table1", Collections.singletonList("col1"),
        Collections.singletonList(null), Collections.emptyMap());
    snc.dropIndex("test1", false);

    snc.createIndex("test2", "table2", Collections.singletonList("col1"),
        Collections.singletonList(null), Collections.emptyMap());
    snc.dropIndex("test2", false);
  }

  @Test
  public void createIndexWithNoneDirectionAndOptions() {
    Map<String, String> options = new HashMap<>();
    options.put("index_type", "unique");

    // should fail for column tables
    try {
      snc.createIndex("test1", "table1", Collections.singletonList("col1"),
          Collections.singletonList(null), options);
      Assert.fail("expected to fail");
    } catch (UnsupportedOperationException expected) {
    }

    snc.createIndex("test2", "table2", Collections.singletonList("col1"),
        Collections.singletonList(null), options);
    snc.dropIndex("test2", false);

    options.put("index_type", "global hash");

    // should fail for column tables
    try {
      snc.createIndex("test1", "table1", Collections.singletonList("col1"),
          Collections.singletonList(null), options);
      Assert.fail("expected to fail");
    } catch (UnsupportedOperationException expected) {
    }
    snc.createIndex("test2", "table2", Collections.singletonList("col1"),
        Collections.singletonList(null), options);
    snc.dropIndex("test2", false);

    // invalid index type should fail for both row and column tables
    options.put("index_type", "bitmap");
    try {
      snc.createIndex("test1", "table1", Collections.singletonList("col1"),
          Collections.singletonList(null), options);
      Assert.fail("expected to fail");
    } catch (UnsupportedOperationException expected) {
    }
    try {
      snc.createIndex("test2", "table2", Collections.singletonList("col1"),
          Collections.singletonList(null), options);
      Assert.fail("expected to fail");
    } catch (Exception e) {
      if (!(e instanceof SQLException) ||
          !((SQLException)e).getSQLState().equals(SQLState.LANG_SYNTAX_ERROR)) {
        Assert.fail("expected syntax error for invalid index type but got " + e);
      }
    }
  }
}

class DummyBeanClass implements Serializable {

  private Integer col1;
  private String col2;

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
