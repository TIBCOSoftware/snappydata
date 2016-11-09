
package io.snappydata.udf;


import java.io.Serializable;
import org.apache.spark.sql.types.DataType;

public interface UDF1<T1, R> extends Serializable {
  public R call(T1 t1) throws Exception;

  public DataType getDataType();
}
