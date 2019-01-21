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


package org.apache.spark.streaming;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Optional;
import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.SchemaDStream;
import org.apache.spark.streaming.api.java.JavaSnappyStreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.util.Utils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;


public class JavaSnappyStreamingContextSuite implements Serializable {

  protected transient JavaSnappyStreamingContext snsc;
  private String master = "local[2]";
  private String appName = this.getClass().getCanonicalName();
  private Duration batchDuration = Milliseconds.apply(500);
  private String sparkHome = "someDir";
  private SparkConf conf = new SparkConf().setMaster(master).setAppName(appName);

  StreamingContextSuite contextSuite = new StreamingContextSuite();

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
    Optional<JavaSnappyStreamingContext> activeSsc = JavaSnappyStreamingContext.getActive();
    if (activeSsc.isPresent()) {
      JavaSnappyStreamingContext jnsc = activeSsc.get();
      jnsc.stop(true, true);
    }
  }


  private void simpleSetup() {

    SparkConf conf = new SparkConf().setMaster(master).setAppName(appName);
    JavaSparkContext sc = new JavaSparkContext(conf);
    snsc = new JavaSnappyStreamingContext(sc, batchDuration);
    Assert.assertTrue(JavaSnappyStreamingContext.getInstance() != null);
  }


  @SuppressWarnings("unchecked")
  @Test
  public void testSimpleConstructor() {

    simpleSetup();

    List<List<String>> inputData = Arrays.asList(
        Arrays.asList("this", "is"),
        Arrays.asList("a", "test"),
        Arrays.asList("counting", "letters"));

    JavaDStream<String> stream = JavaCheckpointTestUtils.attachTestInputStream(snsc, inputData, 1);

    stream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
      @Override
      public void call(JavaRDD<String> rdd) {
        rdd.count();
      }
    });

    snsc.start();

    Assert.assertTrue(JavaSnappyStreamingContext.getActive() != null);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testSchemaDStream() {

    simpleSetup();
    List<List<String>> inputData = Arrays.asList(
        Arrays.asList("this", "is"),
        Arrays.asList("a", "test"),
        Arrays.asList("counting", "letters"));

    JavaDStream<String> stream = JavaCheckpointTestUtils.attachTestInputStream(snsc, inputData, 1);

    SchemaDStream st = snsc.createSchemaDStream(stream, Message.class);

    st.foreachDataFrame(new VoidFunction<Dataset<Row>>() {
      @Override
      public void call(Dataset<Row> rdd) {
        rdd.count();
      }
    });

    snsc.start();

  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetOrCreate() {
    final SparkConf conf = new SparkConf().setMaster(master).setAppName(appName);


    final AtomicBoolean newContextCreated = new AtomicBoolean(false);
    Function0<JavaSnappyStreamingContext> creatingFunction = new
        Function0<JavaSnappyStreamingContext>() {
          @Override
          public JavaSnappyStreamingContext call() throws Exception {
            newContextCreated.set(true);
            return new JavaSnappyStreamingContext(conf, batchDuration);
          }

        };

    String emptyPath = Files.createTempDir().getAbsolutePath();

    // getOrCreate should create new context with empty path
    newContextCreated.set(false);
    snsc = JavaSnappyStreamingContext.getOrCreate(emptyPath, creatingFunction
        ,new Configuration(), false);
    Assert.assertTrue("no context created" , snsc != null);
    Assert.assertTrue("new context not created", newContextCreated.get());
    snsc.stop();
    snsc = null;

    String corruptedCheckpointPath = contextSuite.createCorruptedCheckpoint();

    // getOrCreate should throw exception with fake checkpoint file and createOnError = false
    try {
      snsc = JavaSnappyStreamingContext.getOrCreate(corruptedCheckpointPath, creatingFunction
          ,new Configuration(),
          false);
      Assert.assertTrue("Code should not have reached here", false);
    }catch(Exception e){
      // Success
    }

    // getOrCreate should create new context with fake checkpoint file and createOnError = true

    newContextCreated.set(false);
    snsc = JavaSnappyStreamingContext.getOrCreate(
          corruptedCheckpointPath, creatingFunction, new Configuration(),  true);
    Assert.assertTrue("no context created" , snsc != null);
    Assert.assertTrue("new context not created", newContextCreated.get());
    snsc.stop();
    snsc = null;

    String checkpointPath = contextSuite.createValidCheckpoint();

    // getOrCreate should recover context with checkpoint path, and recover old configuration
    newContextCreated.set(false);
    snsc = JavaSnappyStreamingContext.getOrCreate(checkpointPath, creatingFunction, new Configuration
        (), false);
    Assert.assertTrue("no context created" , snsc != null);
    Assert.assertTrue("old context not recovered", !newContextCreated.get());
    Assert.assertTrue("checkpointed config not " +
        "recovered", snsc.sparkContext().getConf().get("someKey").equals("someValue"));

    snsc.stop();
    snsc = null;
  }

  @Test
  public void testGetActiveOrCreate() throws IOException{

    final SparkConf conf = new SparkConf().setMaster(master).setAppName(appName);
    final AtomicBoolean newContextCreated = new AtomicBoolean(false);
    Function0<JavaSnappyStreamingContext> creatingFunction = new
        Function0<JavaSnappyStreamingContext>() {
          @Override
          public JavaSnappyStreamingContext call() throws Exception {
            newContextCreated.set(true);
            return new JavaSnappyStreamingContext(conf, batchDuration);
          }

        };

    String emptyPath = Files.createTempDir().getAbsolutePath();
    String checkpointPath = contextSuite.createValidCheckpoint();
    String corruptedCheckpointPath = contextSuite.createCorruptedCheckpoint();


    // getActiveOrCreate should return the current active context if there is one
    newContextCreated.set(false);

    SparkConf conf1 = conf.clone().set("spark.streaming.clock", "org.apache.spark.util" +
        ".ManualClock");
    SnappyStreamingContext sncContext = new SnappyStreamingContext(conf1,
        batchDuration);

    snsc = new JavaSnappyStreamingContext(sncContext);

    File testDir = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "spark");
    List<List<String>> expected = fileTestPrepare(testDir);

    JavaDStream<String> input = snsc.textFileStream(testDir.toString());
    JavaTestUtils.attachTestOutputStream(input);
    snsc.start();


    JavaSnappyStreamingContext returnedSsc = JavaSnappyStreamingContext.getActiveOrCreate
        (checkpointPath,
        creatingFunction,
            new Configuration(), false);
    Assert.assertTrue("new context created instead of returning", !newContextCreated.get());
   // Assert.assertTrue("returned context is not the activated context", returnedSsc.eq(snsc));
    snsc.stop();
    snsc = null;

    newContextCreated.set(false);

    // getActiveOrCreate should create new context with empty path
    snsc = JavaSnappyStreamingContext.getActiveOrCreate(emptyPath, creatingFunction,new Configuration
        (), false);
    Assert.assertTrue("no context created" , snsc != null);
    Assert.assertTrue("new context not created", newContextCreated.get());

    snsc.stop();
    snsc = null;


    // getActiveOrCreate should throw exception with fake checkpoint file and createOnError = false
    try {
      snsc = JavaSnappyStreamingContext.getActiveOrCreate(corruptedCheckpointPath, creatingFunction
          , new Configuration(),
          false);
      Assert.assertTrue("Code should not have reached here", false);
    }catch(Exception e){
      // Success
    }

    // getActiveOrCreate should create new context with fake
    // checkpoint file and createOnError = true
    newContextCreated.set(false);
    snsc = JavaSnappyStreamingContext.getActiveOrCreate(corruptedCheckpointPath, creatingFunction
        , new Configuration(),
        true);
    Assert.assertTrue("no context created", snsc != null);
    Assert.assertTrue("new context not created", newContextCreated.get());


    snsc.stop();
    snsc = null;

    // getActiveOrCreate should recover context with checkpoint path, and recover old configuration
    newContextCreated.set(false);
    snsc = JavaSnappyStreamingContext.getActiveOrCreate(checkpointPath, creatingFunction, new Configuration
        (), false);
    Assert.assertTrue("no context created" , snsc != null);
    Assert.assertTrue("old context not recovered", !newContextCreated.get());
    Assert.assertTrue("checkpointed config not " +
        "recovered", snsc.sparkContext().getConf().get("someKey").equals("someValue"));

    snsc.stop();
    snsc = null;

  }

  private static List<List<String>> fileTestPrepare(File testDir) throws IOException {
    File existingFile = new File(testDir, "0");
    Files.write("0\n", existingFile, Charset.forName("UTF-8"));
    Assert.assertTrue(existingFile.setLastModified(1000));
    Assert.assertEquals(1000, existingFile.lastModified());
    return Arrays.asList(Arrays.asList("0"));
  }

  public static class Message {
    public String getMessage() {
      return message;
    }

    public void setMessage(String message) {
      this.message = message;
    }

    private String message;

    public void Message(String message) {
      this.message = message;
    }
  }
}

