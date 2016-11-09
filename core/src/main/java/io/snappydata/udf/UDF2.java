
package io.snappydata.udf;


import java.io.Serializable;

import org.apache.spark.sql.types.DataType;

public interface UDF2<T1, T2, R> extends Serializable {
  public R call(T1 t1, T2 t2) throws Exception;

  public DataType getDataType();
}
