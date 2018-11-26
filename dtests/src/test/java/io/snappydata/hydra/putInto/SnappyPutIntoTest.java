package io.snappydata.hydra.putInto;

import io.snappydata.hydra.cluster.SnappyTest;


public class SnappyPutIntoTest extends SnappyTest {

  public static void HydraTask_concPutIntoUsingJDBCConn() {
    String primaryLocatorHost = getPrimaryLocatorHost();
    String primaryLocatorPort = getPrimaryLocatorPort();
    ConcPutIntoTest.concPutInto(primaryLocatorHost, primaryLocatorPort);
  }
}
