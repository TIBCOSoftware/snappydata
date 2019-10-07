/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package io.snappydata.tools

import scala.collection.mutable.ArrayBuffer

import io.snappydata.impl.LeadImpl
import io.snappydata.{Constant, LocalizedMessages, Property}
import org.scalatest.{Matchers, WordSpec}

import org.apache.spark.sql.SnappyContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * BDD style tests
 */
class LeaderLauncherSpec extends WordSpec with Matchers {

  private def doExtract(param: String, prop: String) = param.toLowerCase.startsWith("-" + prop)

  "leader" when {
    "started" should {
      "have host-data as false" in {
        {

          val l = new LeadImpl
          val opts = l.initStartupArgs((new SparkConf).set(
            Property.McastPort.name, "4958"))

          val hdProp = opts.get(Constant.STORE_PROPERTY_PREFIX +
              com.pivotal.gemfirexd.Attribute.GFXD_HOST_DATA)

          assert(hdProp != null)
          assert(!hdProp.toBoolean)
        }

        {
          val l = new LeadImpl
          val p = (new SparkConf).set(Property.McastPort.name, "4958")
          p.set("host-data", "true")

          val opts = l.initStartupArgs(p)

          val hdProp = opts.get(Constant.STORE_PROPERTY_PREFIX +
              com.pivotal.gemfirexd.Attribute.GFXD_HOST_DATA)

          assert(hdProp != null)
          assert(!hdProp.toBoolean)
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
          val sparkContext = SnappyContext.globalSparkContext
          if (sparkContext != null) sparkContext.stop()

          val l = new LeadImpl
          val conf = (new SparkConf).
              setMaster("local[3]").setAppName("with local master")
          conf.set(Property.McastPort.name, "0")
          conf.set(Constant.STORE_PROPERTY_PREFIX + "host-data", "false")
          val sc = new SparkContext(conf)
          try {
            val opts = l.initStartupArgs(conf, sc)

            val hdProp = opts.get(Constant.STORE_PROPERTY_PREFIX +
                com.pivotal.gemfirexd.Attribute.GFXD_HOST_DATA)

            assert(hdProp != null)
            assert(hdProp.toBoolean)
          } finally {
            sc.stop()
          }
        }

      }

      "always add implicit server group" in {
        {
          val l = new LeadImpl
          val opts = l.initStartupArgs((new SparkConf).set(
            Property.McastPort.name, "4958"))

          val hdProp = opts.get(Constant.STORE_PROPERTY_PREFIX +
              com.pivotal.gemfirexd.Attribute.SERVER_GROUPS)

          assert(hdProp != null)
          assert(hdProp == LeadImpl.LEADER_SERVERGROUP)
        }

        {
          val l = new LeadImpl
          val p = (new SparkConf).set(Property.McastPort.name, "4958")
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
          assert(!hdProp(0).split("=")(1).toBoolean)
        }

        {
          val l = new LeaderLauncher("Test overwrite net server")
          val opts = l.initStartupArgs(ArrayBuffer("start", "-run-netserver=true"))

          val hdProp = opts.filter(doExtract(_, netServerProp))

          assert(hdProp.length == 1)
          assert(!hdProp(0).split("=")(1).toBoolean)
        }
      }

      "assert no zero arg message " in {
        intercept[AssertionError] {
          new LeaderLauncher("Test default net server").initStartupArgs(
            ArrayBuffer(), exitOnEmptyArgs = false)
        }.getMessage.equals(LocalizedMessages.res.getTextMessage("SD_ZERO_ARGS"))
      }

      val replaceString = "<dir>"
      " have jobserver tmp directory redirected " in {
        val l = new LeadImpl
        val conf = l.getConfig(Array.empty)
        val f = conf.getString("spark.jobserver.filedao.rootdir")
        assert(f.indexOf(replaceString) == -1)
        assert(f === "./spark-jobserver/filedao/data")
        val d = conf.getString("spark.jobserver.datadao.rootdir")
        assert(d.indexOf(replaceString) == -1)
        assert(d === "./spark-jobserver/upload")
        val s = conf.getString("spark.jobserver.sqldao.rootdir")
        assert(s.indexOf(replaceString) == -1)
        assert(s === "./spark-jobserver/sqldao/data")
      }

      " have jobserver tmp directory from syshome" in {
        val directory = "/dummy"
        System.setProperty(
          com.pivotal.gemfirexd.internal.iapi.reference.Property.SYSTEM_HOME_PROPERTY, directory)
        val l = new LeadImpl
        val conf = l.getConfig(Array.empty)
        val f = conf.getString("spark.jobserver.filedao.rootdir")
        assert(f.indexOf(replaceString) == -1)
        assert(f startsWith directory)
        val d = conf.getString("spark.jobserver.datadao.rootdir")
        assert(d.indexOf(replaceString) == -1)
        assert(d startsWith directory)
        val s = conf.getString("spark.jobserver.sqldao.rootdir")
        assert(s.indexOf(replaceString) == -1)
        assert(s startsWith directory)
        System.clearProperty(
          com.pivotal.gemfirexd.internal.iapi.reference.Property.SYSTEM_HOME_PROPERTY)
      }
    } // end started
  }

}
