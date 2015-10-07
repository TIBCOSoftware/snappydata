package io.snappydata.tools

import io.snappydata.LocalizedMessages
import org.scalatest.{Matchers, WordSpec}

/**
 * BDD style tests
 *
 * Created by soubhikc on 7/10/15.
 */
class LeaderLauncherSpec extends WordSpec with Matchers {

  def doExtract(param: String, prop: String) = param.toLowerCase.startsWith("-" + prop)

  "leader" when {
    "started" should {
      "have host-data as false" in {
        {
          val l = new LeaderLauncher("Test default host-data")
          val opts = l.initStartupArgs(Array("start"))

          val hdProp = opts.filter(doExtract(_, com.pivotal.gemfirexd.Attribute.GFXD_HOST_DATA))

          assert(hdProp.length == 1)
          assert(hdProp(0).split('=')(1).toBoolean == false)
        }

        {
          val l = new LeaderLauncher("Test override host-data")
          val opts = l.initStartupArgs(Array("start", "host-data=true"))

          val hdProp = opts.filter(doExtract(_, com.pivotal.gemfirexd.Attribute.GFXD_HOST_DATA))

          assert(hdProp.length == 1)
          assert(hdProp(0).split('=')(1).toBoolean == false)
        }

      }

      "always add implicit server group" in {
        {
          val l = new LeaderLauncher("Test default server group")
          val opts = l.initStartupArgs(Array("start"))

          val hdProp = opts.filter(doExtract(_, com.pivotal.gemfirexd.Attribute.SERVER_GROUPS))

          assert(hdProp.length == 1)
          assert(hdProp(0).split('=')(1) == l.LEADER_SERVERGROUP)
        }

        {
          val l = new LeaderLauncher("Test server group addition")
          val opts = l.initStartupArgs(Array("start", "-" + com.pivotal.gemfirexd.Attribute.SERVER_GROUPS + "=DUMMY,GRP"))

          val hdProp = opts.filter(doExtract(_, com.pivotal.gemfirexd.Attribute.SERVER_GROUPS))

          assert(hdProp.length == 1)
          assert(hdProp(0).split('=')(1).endsWith("," + l.LEADER_SERVERGROUP))
        }
      }


      "not start net server" in {
        val netServerProp: String = "run-netserver"

        {
          val l = new LeaderLauncher("Test default net server")
          val opts = l.initStartupArgs(Array("start"))

          val hdProp = opts.filter(doExtract(_, netServerProp))

          assert(hdProp.length == 1)
          assert(hdProp(0).split('=')(1).toBoolean == false)
        }

        {
          val l = new LeaderLauncher("Test overwrite net server")
          val opts = l.initStartupArgs(Array("start", "-run-netserver=true"))

          val hdProp = opts.filter(doExtract(_, netServerProp))

          assert(hdProp.length == 1)
          assert(hdProp(0).split('=')(1).toBoolean == false)
        }
      }

      "assert no zero arg message " in {
        intercept[AssertionError] {
          new LeaderLauncher("Test default net server").initStartupArgs(Array())
        }.getMessage.equals(LocalizedMessages.res.getTextMessage("SD_ZERO_ARGS"))
      }

    } //end started
  }

}
