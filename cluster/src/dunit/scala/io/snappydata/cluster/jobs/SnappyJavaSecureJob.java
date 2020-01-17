package io.snappydata.cluster.jobs;

import com.typesafe.config.Config;
import org.apache.spark.sql.JavaSnappySQLJob;
import org.apache.spark.sql.SnappyJobValid;
import org.apache.spark.sql.SnappyJobValidation;
import org.apache.spark.sql.SnappySession;

public class SnappyJavaSecureJob extends JavaSnappySQLJob {

  @Override
  public SnappyJobValidation isValidJob(SnappySession sc, Config config) {
    SnappyStreamingSecureJob.verifySessionAndConfig(sc, config);
    return new SnappyJobValid();
  }

  @Override
  public Object runSnappyJob(SnappySession sc, Config jobConfig) {
    SnappyStreamingSecureJob.verifySessionAndConfig(sc, jobConfig);
    return "done";
  }
}
