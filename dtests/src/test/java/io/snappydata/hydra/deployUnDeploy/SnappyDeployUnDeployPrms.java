package io.snappydata.hydra.deployUnDeploy;

import java.util.Vector;

import hydra.BasePrms;
import io.snappydata.hydra.cluster.SnappyPrms;

public class SnappyDeployUnDeployPrms extends SnappyPrms {

  /**
   * String value to determine the udf name .
   */
  public static Long udfName;

  public static Long returnType;

  /**
   * Boolean value to determine if exception is expected or not.
   */
  public static Long isExpectedExecption;

  public static Vector getUdfName() {
    Long key = udfName;
    return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
  }

  public static Vector getReturnType() {
    Long key = returnType;
    return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
  }

  public static boolean getIsExpectedExecption() {
    Long key = isExpectedExecption;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  static {
    SnappyPrms.setValues(SnappyDeployUnDeployPrms.class);
  }

  public static void main(String args[]) {
    SnappyPrms.dumpKeys();
  }
}
