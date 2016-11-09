package io.snappydata.udf;

import java.io.Serializable;

import org.apache.spark.sql.types.DataType;

public interface UDF6<T1, T2, T3, T4, T5, T6, R> extends Serializable {
  public R call(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, T6 t6) throws Exception;

  public DataType getDataType();
}
