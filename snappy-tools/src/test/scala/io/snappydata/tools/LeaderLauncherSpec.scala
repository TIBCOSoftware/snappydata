package io.snappydata.tools

import java.util.Properties

import scala.collection.mutable.ArrayBuffer

import com.pivotal.gemfirexd.internal.engine.fabricservice.FabricServiceImpl
import io.snappydata.impl.LeadImpl
import io.snappydata.{Constant, LocalizedMessages, Property}
import org.scalatest.{Matchers, WordSpec}

import org.apache.spark.sql.SnappyContext
import org.apache.spark.{SparkConf, SparkContext}

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
          val opts = l.initStartupArgs((new SparkConf).set(Property.mcastPort, "4958"))

          val hdProp = opts.get(Constant.STORE_PROPERTY_PREFIX +
              com.pivotal.gemfirexd.Attribute.GFXD_HOST_DATA)

          assert(hdProp != null)
          assert(hdProp.toBoolean == false)
        }

        {
          val l = new LeadImpl
          val p = (new SparkConf).set(Property.mcastPort, "4958")
          p.set("host-data", "true")

          val opts = l.initStartupArgs(p)

          val hdProp = opts.get(Constant.STORE_PROPERTY_PREFIX +
              com.pivotal.gemfirexd.Attribute.GFXD_HOST_DATA)

          assert(hdProp != null)
          assert(hdProp.toBoolean == false)
        }

      }

      "have host-data true for loner" in {

//        {
//
//          val l = new LeadImpl
//          val conf = new SparkConf().
//              setMaster(s"${Constant.JDBC_URL_PREFIX}${Property.mcastPort}=0").
//              setAppName("check hostdata true")
//          val sc = new SparkContext(conf)
//          try {
//            val opts = l.initStartupArgs(conf, sc)
//
//            val hdProp = opts.get(Constant.STORE_PROPERTY_PREFIX +
//                com.pivotal.gemfirexd.Attribute.GFXD_HOST_DATA)
//
//            assert(hdProp != null)
//            assert(hdProp.toBoolean == true)
//          } finally {
//            sc.stop()
//          }
//        }

        {
          // Stop if already any present
          SnappyContext.stop()

          val l = new LeadImpl
          val conf = (new SparkConf).
              setMaster("local[3]").setAppName("with local master")
          conf.set(Property.mcastPort, "0")
          conf.set(Constant.STORE_PROPERTY_PREFIX + "host-data", "false")
          val sc = new SparkContext(conf)
          try {
            val opts = l.initStartupArgs(conf, sc)

            val hdProp = opts.get(Constant.STORE_PROPERTY_PREFIX +
                com.pivotal.gemfirexd.Attribute.GFXD_HOST_DATA)

            assert(hdProp != null)
            assert(hdProp.toBoolean == true)
          } finally {
            sc.stop()
          }
        }

      }

      "always add implicit server group" in {
        {
          val l = new LeadImpl
          val opts = l.initStartupArgs((new SparkConf).set(Property.mcastPort, "4958"))

          val hdProp = opts.get(Constant.STORE_PROPERTY_PREFIX +
              com.pivotal.gemfirexd.Attribute.SERVER_GROUPS)

          assert(hdProp != null)
          assert(hdProp == LeadImpl.LEADER_SERVERGROUP)
        }

        {
          val l = new LeadImpl
          val p = (new SparkConf).set(Property.mcastPort, "4958")
          p.set(Constant.STORE_PROPERTY_PREFIX +
              com.pivotal.gemfirexd.Attribute.SERVER_GROUPS, "DUMMY,GRP")
          val opts = l.initStartupArgs(p)

          val hdProp = opts.get(Constant.STORE_PROPERTY_PREFIX +
              com.pivotal.gemfirexd.Attribute.SERVER_GROUPS)

          assert(hdProp != null)
          assert(hdProp.endsWith("," + LeadImpl.LEADER_SERVERGROUP))
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
