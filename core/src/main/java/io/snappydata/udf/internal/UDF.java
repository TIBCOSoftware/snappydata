package io.snappydata.udf.internal;

import java.io.Serializable;

import org.apache.spark.sql.types.DataType;

/**
 * Created by rishim on 4/11/16.
 */
public interface UDF extends Serializable{

  public DataType getDataType();
}
