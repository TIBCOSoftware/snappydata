package io.snappydata.dunit


import dunit.{SerializableRunnable, Host, DistributedTestBase}
import junit.framework.AssertionFailedError


/**
 * This dunit test provides a very basic template of how a dunit in scala would be like.
 *
 * Created by amogh on 14/10/15.
 */
class DemoScalaDUnitTest(val s: String) extends DistributedTestBase(s) {

  import DemoScalaDUnitTest._

  val host = Host.getHost(0);

  val vm0 = host.getVM(0);
  val vm1 = host.getVM(1);
  val vm2 = host.getVM(2);
  val vm3 = host.getVM(3);

  override
  def setUp(): Unit = {
    //super.setUp()
  }

  override
  def tearDown2(): Unit = {
  }

  def testHelloWorld(): Unit = {
    helloWorld()
  }

  def testHelloWorldOnVM(): Unit = {
    val arg = new Array[AnyRef](1)
    arg(0) = "testHelloWorldOnVM: Hello World!"
    vm1.invoke(this.getClass, "hello", arg)
  }

  def testHelloWorldOnVMViaRunnable(): Unit = {
    vm0.invoke(new SerializableRunnable() {
      def run {
        hello("testHelloWorldOnVMViaRunnable: Hello World!")
      }
    })
  }

  def testHelloWorldFailure(): Unit = {
    try {
      DistributedTestBase.fail("!!! TEST Failed !!!", new Exception)
    } catch {
      case err: AssertionFailedError => hello("testHelloWorldFailure: Test failed as expected!")
      case _: Throwable => DistributedTestBase.fail("!!! Test failed but with different error !!!", null)
    }
  }
}

object DemoScalaDUnitTest {

  def helloWorld(): Unit = {
    hello("Hello World! " + this.getClass);
  }

  def hello(s: String): Unit = {
    println(s);
  }
}