
package io.snappydata.udf;

import io.snappydata.udf.internal.UDF;


public interface UDF2<T1, T2, R> extends UDF {
  public R call(T1 t1, T2 t2) throws Exception;
}
