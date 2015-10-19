package io.snappydata

import com.pivotal.gemfirexd.FabricService.State
import com.pivotal.gemfirexd.internal.engine.fabricservice.FabricServiceImpl
import com.pivotal.gemfirexd.{FabricLocator, FabricServer}
import io.snappydata.gemxd.ClusterCallback

// TODO: Documentation
trait Server extends FabricServer with ClusterCallback {

}

// TODO: Documentation
trait Lead extends Server {
  val LEADER_SERVERGROUP = "IMPLICIT_LEADER_SERVERGROUP"

  @volatile protected[this] var primaryServiceStatus = State.UNINITIALIZED

  /**
   * This method will start additional leader service(s) if elected primary and must be called
   * after [[io.snappydata.Lead#start]].
   *
   * If during the invocation os this method there is already an existing primary lead node elected,
   * it will immediately return. [[io.snappydata.Lead#getPrimaryServiceStatus]] will indicate
   * [[State.STANDBY]] status.
   *
   * User can invoke [[io.snappydata.Lead#waitForPrimaryDeparture]] at any point in time to wait for
   * STANDBY node to become primary during failover.
   *
   * <code>
   *   val leadInst = ServiceManager.getLeadInstance
   *
   *   do {
   *      leadInst.startPrimaryServices()
   *      leadInst.getPrimaryServiceStatus() match {
   *          case State.STARTING =>
   *              Thread.sleep(1000)
   *          case State.STANDBY =>
   *              // System.io.println(STANDBY)
   *              leadInst.waitForPrimaryDeparture()
   *          case State.RUNNING =>
   *               System.io.println(RUNNING)
   *               return
   *          case v =>
   *               System.io.println("Unhandled Leader Primary Service Status ")
   *       }
   *   } while (true)
   * </code>
   *
   * @throws java.sql.SQLException
   */
  @throws(classOf[Exception])
  def startPrimaryServices()

  /**
   * Returns the present status of the primary service startup status.
   */
  def getPrimaryServiceStatus() = primaryServiceStatus

  /**
   * A blocking call to this method will wait indefinitely until the primary lead node goes down.
   *
   * Once this method returns, user need to call [[io.snappydata.Lead.startPrimaryServices]] again for starting
   * the services on this node. If there are multiple STANDBY nodes, one of them will succeed and others will
   * get into a STANDBY mode again.
   *
   * @throws java.lang.Exception
   */
  @throws(classOf[Exception])
  def waitForPrimaryDeparture()

}

// TODO: Documentation
trait Locator extends FabricLocator with ClusterCallback {

}

/**
 * Created by soubhikc on 16/10/15.
 */
trait ProtocolOverrides extends FabricServiceImpl {

  abstract override def getProtocol: java.lang.String = {
    "jdbc:snappydata:"
  }

  abstract override def getNetProtocol = {
    "jdbc:snappydata:"
  }

  abstract override def getDRDAProtocol: String = {
    return "jdbc:snappydata:drda://"
  }

  abstract override def getThriftProtocol: String = {
    return "jdbc:snappydata:thrift://"
  }
}
