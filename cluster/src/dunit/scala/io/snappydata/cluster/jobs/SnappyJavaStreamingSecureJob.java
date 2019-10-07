package io.snappydata.cluster.jobs;

import com.typesafe.config.Config;
import org.apache.spark.sql.SnappyJobValid;
import org.apache.spark.sql.SnappyJobValidation;
import org.apache.spark.streaming.JavaSnappyStreamingJob;
import org.apache.spark.streaming.api.java.JavaSnappyStreamingContext;

public class SnappyJavaStreamingSecureJob extends JavaSnappyStreamingJob {
  @Override
  public Object runSnappyJob(JavaSnappyStreamingContext snc, Config jobConfig) {
    SnappyStreamingSecureJob.verifySessionAndConfig(snc.snappySession(), jobConfig);
    return "done";
  }

  @Override
  public SnappyJobValidation isValidJob(JavaSnappyStreamingContext snc, Config jobConfig) {
    SnappyStreamingSecureJob.verifySessionAndConfig(snc.snappySession(), jobConfig);
    return new SnappyJobValid();
  }
}
