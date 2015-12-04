package io.snappydata

import java.util.Properties

import com.pivotal.gemfirexd.internal.engine.fabricservice.FabricServiceImpl
import org.apache.spark.sql.SnappyContext

/**
 * Base abstract class for all SnappyData tests similar to SnappyFunSuite.
 *
 * Created by skumar on 4/12/15.
 */
abstract class SnappyToolFunSuite
    extends SnappyFunSuite {// scalastyle:ignore

  // In future we also need to make sure that different test can have different
  // spark conf, sql conf to start with
  override def afterAll(): Unit = {
    super.afterAll()
    SnappyContext.stop()
    FabricServiceImpl.getInstance.stop(new Properties())
  }
}
