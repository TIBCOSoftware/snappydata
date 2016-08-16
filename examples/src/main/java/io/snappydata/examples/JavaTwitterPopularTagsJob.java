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

package io.snappydata.examples;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import com.typesafe.config.Config;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SnappyJobValid;
import org.apache.spark.sql.SnappyJobValidation;
import org.apache.spark.sql.streaming.SchemaDStream;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.JavaSnappyStreamingJob;
import org.apache.spark.streaming.api.java.JavaSnappyStreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;

import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.createStructField;

/**
 * Run this on your local machine:
 * <p/>
 * `$ sbin/snappy-start-all.sh`
 * <p/>
 * To run with live twitter streaming, export twitter credentials
 * `$ export APP_PROPS="consumerKey=<consumerKey>,consumerSecret=<consumerSecret>, \
 * accessToken=<accessToken>,accessTokenSecret=<accessTokenSecret>"`
 * <p/>
 * `$ ./bin/snappy-job.sh submit --lead localhost:8090 \
 * --app-name JavaTwitterPopularTagsJob --class io.snappydata.examples.JavaTwitterPopularTagsJob \
 * --app-jar $SNAPPY_HOME/lib/quickstart-0.1.0-SNAPSHOT.jar --stream`
 * <p/>
 * To run with stored twitter data, run simulateTwitterStream after the Job is submitted:
 * `$ ./quickstart/scripts/simulateTwitterStream`
 */


public class JavaTwitterPopularTagsJob extends JavaSnappyStreamingJob {
  @Override
  public Object runSnappyJob(JavaSnappyStreamingContext snsc, Config jobConfig) {

    JavaDStream stream = null;
    PrintWriter pw = null;
    String currentDirectory = null;
    boolean success = false;
    String outFileName = null;

    try {
      currentDirectory = new java.io.File(".").getCanonicalPath();
      outFileName = String.format("JavaTwitterPopularTagsJob-%d.out", System.currentTimeMillis());
      pw = new PrintWriter(outFileName);

      StructType schema = new StructType(new StructField[]{
          createStructField("hashtag", StringType, false)
      });

      snsc.snappyContext().sql("DROP TABLE IF EXISTS topktable");
      snsc.snappyContext().sql("DROP TABLE IF EXISTS hashtagtable");
      snsc.snappyContext().sql("DROP TABLE IF EXISTS retweettable");


      if (jobConfig.hasPath("consumerKey") && jobConfig.hasPath("consumerKey")
          && jobConfig.hasPath("accessToken") && jobConfig.hasPath("accessTokenSecret")) {
        pw.println("##### Running example with live twitter stream #####");

        String consumerKey = jobConfig.getString("consumerKey");
        String consumerSecret = jobConfig.getString("consumerSecret");
        String accessToken = jobConfig.getString("accessToken");
        String accessTokenSecret = jobConfig.getString("accessTokenSecret");

        // Create twitter stream table
        snsc.sql("CREATE STREAM TABLE hashtagtable (hashtag STRING) USING " +
            "twitter_stream OPTIONS (" +
            String.format("consumerKey '%s',", consumerKey) +
            String.format("consumerSecret '%s',", consumerSecret) +
            String.format("accessToken '%s',", accessToken) +
            String.format("accessTokenSecret '%s',", accessTokenSecret) +
            String.format("rowConverter '%s'", "org.apache.spark.sql.streaming.TweetToHashtagRow")
                +")"
            );

        snsc.sql("CREATE STREAM TABLE retweettable (retweetId LONG, retweetCnt INT, " +
            "retweetTxt STRING) USING twitter_stream OPTIONS (" +
            String.format("consumerKey '%s',", consumerKey) +
            String.format("consumerSecret '%s',", consumerSecret) +
            String.format("accessToken '%s',", accessToken) +
            String.format("accessTokenSecret '%s',", accessTokenSecret) +
            String.format("rowConverter '%s'", "org.apache.spark.sql.streaming.TweetToRetweetRow")
                +")"
          );

      } else {
        // Create file stream table
        pw.println("##### Running example with stored tweet data #####");
        snsc.sql("CREATE STREAM TABLE hashtagtable (hashtag STRING) USING file_stream " +
            "OPTIONS (storagelevel 'MEMORY_AND_DISK_SER_2', " +
            "rowConverter 'org.apache.spark.sql.streaming.TweetToHashtagRow'," +
            "directory '/tmp/copiedtwitterdata')");

        snsc.sql("CREATE STREAM TABLE retweettable (retweetId LONG, retweetCnt INT, " +
            "retweetTxt STRING) USING file_stream " +
            "OPTIONS (storagelevel 'MEMORY_AND_DISK_SER_2', " +
            "rowConverter 'org.apache.spark.sql.streaming.TweetToRetweetRow'," +
            "directory '/tmp/copiedtwitterdata')");
      }

      // Register continuous queries on the tables and specify window clauses
      SchemaDStream retweetStream = snsc.registerCQ("SELECT * FROM retweettable " +
          "WINDOW (DURATION 2 SECONDS, SLIDE 2 SECONDS)");

      Map topKOption = new HashMap();
      topKOption.put("epoch", new Long(System.currentTimeMillis()).toString());
      topKOption.put("timeInterval", "2000ms");
      topKOption.put("size", "10");


      // Create TopK table on the base stream table which is hashtagtable
      // TopK object is automatically populated from the stream table
      snsc.snappyContext().createApproxTSTopK("topktable", "hashtagtable",
          "hashtag", schema, topKOption, false);

      final String tableName = "retweetStore";

      snsc.snappyContext().dropTable(tableName, true);

      // Create row table to insert retweets based on retweetId as Primary key
      // When a tweet is retweeted multiple times, the previous entry of the tweet
      // is over written by the new retweet count.
      snsc.snappyContext().sql(String.format("CREATE TABLE %s (retweetId BIGINT PRIMARY KEY, " +
          "retweetCnt INT, retweetTxt STRING) USING row OPTIONS ()",tableName));

      // Save data in snappy store
      retweetStream.foreachDataFrame(new VoidFunction<DataFrame>() {
        @Override
        public void call(DataFrame df) {
          df.write().insertInto(tableName);
        }
      });
      snsc.start();


      int runTime;

      if (jobConfig.hasPath("streamRunTime")) {
        runTime = Integer.parseInt(jobConfig.getString("streamRunTime")) * 1000;
      } else {
        runTime = 120 * 1000;
      }

      long end = System.currentTimeMillis() + runTime;

      while (end > System.currentTimeMillis()) {
        Thread.sleep(2000);
        pw.println("\n******** Top 10 hash tags of last two seconds *******\n");

        // Query the topk structure for the popular hashtags of last two seconds
        Row[] result = snsc.snappyContext().queryApproxTSTopK("topktable",
            System.currentTimeMillis() - 2000, System.currentTimeMillis())
            .collect();

        printResult(result, pw);

      }

      // Query the topk structure for the popular hashtags of until now
      Row[] result = snsc.sql("SELECT * FROM topktable").collect();
      printResult(result, pw);

      // Query the snappystore Row table to find out the top retweets
      pw.println("\n####### Top 10 popular tweets - Query Row table #######\n");
      result = snsc.snappyContext().sql("SELECT retweetId AS RetweetId, " +
          "retweetCnt AS RetweetsCount, retweetTxt AS Text FROM " + tableName +
          " ORDER BY RetweetsCount DESC LIMIT 10")
          .collect();

      printResult(result, pw);

      pw.println("\n#######################################################");

     success = true;
    } catch (IOException e) {
      //pw.close();
    } catch (InterruptedException e) {
      //pw.close();
    } finally {
      pw.close();

      snsc.stop(false, true);
    }
    return "See "+ currentDirectory +"/" + outFileName;
  }

  private void printResult(Row[] result, PrintWriter pw) {
    for (Row row : result) {
      pw.println(row.toString());
    }
  }

  @Override
  public SnappyJobValidation isValidJob(JavaSnappyStreamingContext snc, Config jobConfig) {
    return new SnappyJobValid();
  }
}
