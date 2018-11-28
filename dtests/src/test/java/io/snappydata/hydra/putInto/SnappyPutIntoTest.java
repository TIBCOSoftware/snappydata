package io.snappydata.hydra.putInto;

import hydra.TestConfig;
import io.snappydata.hydra.cluster.SnappyPrms;
import io.snappydata.hydra.cluster.SnappyTest;


public class SnappyPutIntoTest extends SnappyTest {

  public static int numThreads = TestConfig.tasktab().intAt(SnappyPrms.numThreadsForConcExecution, TestConfig.tab().
      intAt(SnappyPrms.numThreadsForConcExecution, 15));

  public static void HydraTask_concPutIntoUsingJDBCConn() {

    String primaryLocatorHost = getPrimaryLocatorHost();
    String primaryLocatorPort = getPrimaryLocatorPort();
    ConcPutIntoTest.concPutInto(primaryLocatorHost, primaryLocatorPort, numThreads);
  }

  public static void HydraTask_concSelectUsingJDBCConn() {
    String primaryLocatorHost = getPrimaryLocatorHost();
    String primaryLocatorPort = getPrimaryLocatorPort();
    ConcPutIntoTest.conSelect(primaryLocatorHost, primaryLocatorPort, numThreads);
  }

}
