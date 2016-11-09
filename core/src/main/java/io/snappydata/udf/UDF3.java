package io.snappydata.udf;


import java.io.Serializable;

import org.apache.spark.sql.types.DataType;

public interface UDF3<T1, T2, T3, R> extends Serializable {
  public R call(T1 t1, T2 t2, T3 t3) throws Exception;

  public DataType getDataType();
}
