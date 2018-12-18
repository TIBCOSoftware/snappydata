package io.snappydata.examples;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import com.typesafe.config.Config;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SnappyJobValid;
import org.apache.spark.sql.SnappyJobValidation;
import org.apache.spark.sql.streaming.SchemaDStream;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.JavaSnappyStreamingJob;
import org.apache.spark.streaming.api.java.JavaSnappyStreamingContext;

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
 * --app-jar $SNAPPY_HOME/examples/jars/quickstart.jar --stream`
 * <p/>
 * To run with stored twitter data, run simulateTwitterStream after the Job is submitted:
 * `$ ./quickstart/scripts/simulateTwitterStream`
 */


public class JavaTwitterPopularTagsJob extends JavaSnappyStreamingJob {
  @Override
  public Object runSnappyJob(JavaSnappyStreamingContext snsc, Config jobConfig) {

    String currentDirectory = null;
    String outFileName = String.format(
        "JavaTwitterPopularTagsJob-%d.out", System.currentTimeMillis());

    try (PrintWriter pw = new PrintWriter(outFileName)) {
      currentDirectory = new java.io.File(".").getCanonicalPath();

      StructType schema = new StructType(new StructField[]{
          createStructField("hashtag", StringType, false)
      });

      snsc.snappySession().sql("DROP TABLE IF EXISTS topktable");
      snsc.snappySession().sql("DROP TABLE IF EXISTS hashtagtable");
      snsc.snappySession().sql("DROP TABLE IF EXISTS retweettable");


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
            + ")"
        );

        snsc.sql("CREATE STREAM TABLE retweettable (retweetId LONG, retweetCnt INT, " +
            "retweetTxt STRING) USING twitter_stream OPTIONS (" +
            String.format("consumerKey '%s',", consumerKey) +
            String.format("consumerSecret '%s',", consumerSecret) +
            String.format("accessToken '%s',", accessToken) +
            String.format("accessTokenSecret '%s',", accessTokenSecret) +
            String.format("rowConverter '%s'", "org.apache.spark.sql.streaming.TweetToRetweetRow")
            + ")"
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

      Map<String, String> topKOption = new HashMap<>();
      topKOption.put("epoch", Long.toString(System.currentTimeMillis()));
      topKOption.put("timeInterval", "2000ms");
      topKOption.put("size", "10");


      // Create TopK table on the base stream table which is hashtagtable
      // TopK object is automatically populated from the stream table
      snsc.snappySession().createApproxTSTopK("topktable", "hashtagtable",
          "hashtag", schema, topKOption, false);

      final String tableName = "retweetStore";

      snsc.snappySession().dropTable(tableName, true);

      // Create row table to insert retweets based on retweetId as Primary key
      // When a tweet is retweeted multiple times, the previous entry of the tweet
      // is over written by the new retweet count.
      snsc.snappySession().sql(String.format("CREATE TABLE %s (retweetId BIGINT PRIMARY KEY, " +
          "retweetCnt INT, retweetTxt STRING) USING row OPTIONS ()", tableName));

      // Save data in snappy store
      retweetStream.foreachDataFrame(new VoidFunction<Dataset<Row>>() {
        @Override
        public void call(Dataset<Row> df) {
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
        List<Row> result = snsc.snappySession().queryApproxTSTopK("topktable",
            System.currentTimeMillis() - 2000, System.currentTimeMillis(), -1)
            .collectAsList();

        printResult(result, pw);

      }

      // Query the topk structure for the popular hashtags of until now
      List<Row> result = snsc.sql("SELECT * FROM topktable").collectAsList();
      printResult(result, pw);

      // Query the snappystore Row table to find out the top retweets
      pw.println("\n####### Top 10 popular tweets - Query Row table #######\n");
      result = snsc.snappySession().sql("SELECT retweetId AS RetweetId, " +
          "retweetCnt AS RetweetsCount, retweetTxt AS Text FROM " + tableName +
          " ORDER BY RetweetsCount DESC LIMIT 10")
          .collectAsList();

      printResult(result, pw);

      pw.println("\n#######################################################");
    } catch (IOException | InterruptedException e) {
      StringWriter sw = new StringWriter();
      PrintWriter spw = new PrintWriter(sw);
      spw.println("ERROR: failed with " + e);
      e.printStackTrace(spw);
      return spw.toString();
    } finally {
      snsc.stop(false, true);
    }
    return "See " + currentDirectory + "/" + outFileName;
  }

  private void printResult(List<Row> result, PrintWriter pw) {
    for (Row row : result) {
      pw.println(row.toString());
    }
  }

  @Override
  public SnappyJobValidation isValidJob(JavaSnappyStreamingContext snc, Config jobConfig) {
    return new SnappyJobValid();
  }
}
