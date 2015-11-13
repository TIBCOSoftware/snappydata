package io.snappydata.tools

import java.util.Properties

import io.snappydata.impl.LeadImpl

import scala.collection.mutable.ArrayBuffer

import io.snappydata.LocalizedMessages
import org.scalatest.{Matchers, WordSpec}

import org.apache.spark.SparkConf

/**
 * BDD style tests
 *
 * Created by soubhikc on 7/10/15.
 */
class LeaderLauncherSpec extends WordSpec with Matchers {

  private def doExtract(param: String, prop: String) = param.toLowerCase.startsWith("-" + prop)

  "leader" when {
    "started" should {
      "have host-data as false" in {
        {

          val l = new LeadImpl
          val opts = l.initStartupArgs(new SparkConf)

          val hdProp = opts.get(com.pivotal.gemfirexd.Attribute.GFXD_HOST_DATA)

          assert(hdProp != null)
          assert(hdProp.toBoolean == false)
        }

        {
          val l = new LeadImpl
          val p = new SparkConf()
          p.set("host-data", "true")

          val opts = l.initStartupArgs(p)

          val hdProp = opts.get(com.pivotal.gemfirexd.Attribute.GFXD_HOST_DATA)

          assert(hdProp != null)
          assert(hdProp.toBoolean == false)
        }

      }

      "always add implicit server group" in {
        {
          val l = new LeadImpl
          val opts = l.initStartupArgs(new SparkConf())

          val hdProp = opts.get(com.pivotal.gemfirexd.Attribute.SERVER_GROUPS)

          assert(hdProp != null)
          assert(hdProp == l.LEADER_SERVERGROUP)
        }

        {
          val l = new LeadImpl
          val p = new SparkConf()
          p.set(com.pivotal.gemfirexd.Attribute.SERVER_GROUPS, "DUMMY,GRP")
          val opts = l.initStartupArgs(p)

          val hdProp = opts.get(com.pivotal.gemfirexd.Attribute.SERVER_GROUPS)

          assert(hdProp != null)
          assert(hdProp.endsWith("," + l.LEADER_SERVERGROUP))
        }
      }


      "not start net server" in {
        val netServerProp: String = "run-netserver"

        {
          val l = new LeaderLauncher("Test default net server")
          val opts = l.initStartupArgs(ArrayBuffer("start"))

          val hdProp = opts.filter(doExtract(_, netServerProp))

          assert(hdProp.length == 1)
          assert(hdProp(0).split("=")(1).toBoolean == false)
        }

        {
          val l = new LeaderLauncher("Test overwrite net server")
          val opts = l.initStartupArgs(ArrayBuffer("start", "-run-netserver=true"))

          val hdProp = opts.filter(doExtract(_, netServerProp))

          assert(hdProp.length == 1)
          assert(hdProp(0).split("=")(1).toBoolean == false)
        }
      }

      "assert no zero arg message " in {
        intercept[AssertionError] {
          new LeaderLauncher("Test default net server").initStartupArgs(ArrayBuffer())
        }.getMessage.equals(LocalizedMessages.res.getTextMessage("SD_ZERO_ARGS"))
      }

    } // end started
  }

}
