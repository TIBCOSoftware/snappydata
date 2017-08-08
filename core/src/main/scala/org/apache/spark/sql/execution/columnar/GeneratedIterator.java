package org.apache.spark.sql.execution.columnar;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.execution.columnar.encoding.ColumnDecoder;
import org.apache.spark.sql.execution.columnar.encoding.ColumnEncoder;
import org.apache.spark.sql.execution.columnar.encoding.DeltaWriter;
import org.apache.spark.unsafe.types.UTF8String;
