package org.apache.spark.sql;

import com.typesafe.config.Config;
import spark.jobserver.SparkJobValidation;

/**
 * Created by rishim on 29/4/16.
 */
public abstract class JavaSnappySQLJob  implements SnappySQLJob {

  abstract public  Object runJavaJob(SnappyContext snc, Config jobConfig);

  abstract public JSparkJobValidation isValidJob(SnappyContext snc, Config jobConfig);

  @Override
  public Object runJob(Object sc, Config jobConfig) {
    return runJavaJob((SnappyContext)sc, jobConfig);
  }

  @Override
  public SparkJobValidation validate(Object sc, Config config) {
    JSparkJobValidation  status = isValidJob((SnappyContext)sc, config);
    return JavaJobValidate.validate(status);
  }
}
