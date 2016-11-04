package io.snappydata.udf;


import io.snappydata.udf.internal.UDF;

public interface UDF3<T1, T2, T3, R> extends UDF {
  public R call(T1 t1, T2 t2, T3 t3) throws Exception;
}
