/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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

package io.snappydata.hydra.complexdatatypes

import java.io.{File, FileOutputStream, PrintWriter}

import com.typesafe.config.Config
import io.snappydata.hydra.SnappyTestUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql._

class AllMixedTypes extends SnappySQLJob{
  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()

  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {

    // scalastyle:off println
    println("AllMixedType Job started...")

    val snc : SnappyContext = snappySession.sqlContext
    val spark : SparkSession = SparkSession.builder().getOrCreate()
    val sc : SparkContext = SparkContext.getOrCreate()
    val sqlContext : SQLContext = SQLContext.getOrCreate(sc)
    val printContent : Boolean = false

    def getCurrentDirectory : String = new File(".").getCanonicalPath
    val outputFile : String = "ValidateAllMixedTypes" + "_" + "column" +
      System.currentTimeMillis() + jobConfig.getString("logFileName")
    val pw : PrintWriter = new PrintWriter(new FileOutputStream(new File(outputFile), false))

    val Mixed_Q1 = "SELECT * FROM T20.TwentyTwenty ORDER BY name"
    val Mixed_Q2 = "SELECT name, " +
             "SUM(LastThreeMatchPerformance[0] + LastThreeMatchPerformance[1] + " +
             "LastThreeMatchPerformance[2]) AS RunsScored " +
             "FROM T20.TwentyTwenty WHERE Roll[1] = 'WicketKeeper' GROUP BY name"
    val Mixed_Q3 = "SELECT name, LastThreeMatchPerformance[2] AS RunsScoredinLastMatch, " +
             "Profile.Matches,Profile.SR,Profile.Runs " +
             "FROM T20.TwentyTwenty WHERE Profile.Runs >= 1000 ORDER BY Profile.Runs DESC"
    val Mixed_Q4 = "SELECT COUNT(*) AS AllRounder FROM " +
      "T20.TwentyTwenty WHERE Roll[2] = 'AllRounder'"
    val Mixed_Q5 = "SELECT name, Profile.SR,Profile.Runs " +
      "FROM T20.TwentyTwenty ORDER BY Profile.SR DESC"

    /* --- Snappy Job --- */
    snc.sql("CREATE SCHEMA T20")

    snc.sql("CREATE TABLE IF NOT EXISTS T20.TwentyTwenty(name String, " +
      "LastThreeMatchPerformance ARRAY<Double>, " +
      "Roll MAP<SMALLINT,STRING>, " +
      "Profile STRUCT<Matches:Long,Runs:Int,SR:Double,isPlaying:Boolean>) USING column")

    snc.sql("INSERT INTO T20.TwentyTwenty SELECT " +
                     "'M S Dhoni',ARRAY(37,25,58),MAP(1,'WicketKeeper')," +
                     "STRUCT(93,1487,127.09,true)")
    snc.sql("INSERT INTO T20.TwentyTwenty SELECT " +
                     "'Yuvaraj Singh',ARRAY(68,72,21),MAP(2,'AllRounder')," +
                     "STRUCT(58,1177,136.38,false)")
    snc.sql("INSERT INTO T20.TwentyTwenty SELECT " +
                     "'Viral Kohli',ARRAY(52,102,23),MAP(3,'Batsmen'),STRUCT(65,2167,136.11,true)")
    snc.sql("INSERT INTO T20.TwentyTwenty SELECT " +
                     "'Gautam Gambhir',ARRAY(35,48,74),MAP(3,'Batsmen')," +
                     "STRUCT(37,932,119.02,false)")
    snc.sql("INSERT INTO T20.TwentyTwenty SELECT " +
                     "'Rohit Sharma',ARRAY(0,56,44),MAP(3,'Batsmen'),STRUCT(90,2237,138.17,true)")
    snc.sql("INSERT INTO T20.TwentyTwenty SELECT " +
                     "'Ravindra Jadeja',ARRAY(15,25,33),MAP(2,'AllRounder')," +
                     "STRUCT(40,116,93.54,true)")
    snc.sql("INSERT INTO T20.TwentyTwenty SELECT " +
                     "'Virendra Sehwag',ARRAY(5,45,39),MAP(3,'Batsmen')," +
                     "STRUCT(19,394,145.39,false)")
    snc.sql("INSERT INTO T20.TwentyTwenty SELECT " +
                     "'Hardik Pandya',ARRAY(27,14,19),MAP(2,'AllRounder')," +
                     "STRUCT(35,271,153.10,true)")
    snc.sql("INSERT INTO T20.TwentyTwenty SELECT " +
                     "'Suresh Raina',ARRAY(31,26,48),MAP(3,'Batsmen'),STRUCT(78,1605,134.87,false)")
    snc.sql("INSERT INTO T20.TwentyTwenty SELECT " +
                     "'Harbhajan Singh',ARRAY(23,5,11),MAP(4,'Bowler'),STRUCT(28,108,124.13,false)")
    snc.sql("INSERT INTO T20.TwentyTwenty SELECT " +
                     "'Ashish Nehra',ARRAY(2,1,5),MAP(4,'Bowler'),STRUCT(27,28,71.79,false)")
    snc.sql("INSERT INTO T20.TwentyTwenty SELECT " +
                     "'Kuldeep Yadav',ARRAY(3,3,0),MAP(4,'Bowler'),STRUCT(17,20,100.0,true)")
    snc.sql("INSERT INTO T20.TwentyTwenty SELECT " +
                     "'Parthiv Patel',ARRAY(29,18,9),MAP(1,'WicketKeeper')," +
                     "STRUCT(2,36,112.50,false)")
    snc.sql("INSERT INTO T20.TwentyTwenty SELECT " +
                     "'Ravichandran Ashwin',ARRAY(15,7,12),MAP(4,'Bowler')," +
                     "STRUCT(46,123,106.95,true)")
    snc.sql("INSERT INTO T20.TwentyTwenty SELECT " +
                     "'Irfan Pathan',ARRAY(17,23,18),MAP(2,'AllRounder')," +
                     "STRUCT(24,172,119.44,false)")

    snc.sql(Mixed_Q1)
    snc.sql(Mixed_Q2)
    snc.sql(Mixed_Q3)
    snc.sql(Mixed_Q4)
    snc.sql(Mixed_Q5)

    if(printContent) {
      println("snc : Mixed_Q1 " + (snc.sql(Mixed_Q1).show))
      println("snc : Mixed_Q2 " + (snc.sql(Mixed_Q2).show))
      println("snc : Mixed_Q3 " + (snc.sql(Mixed_Q3).show))
      println("snc : Mixed_Q4 " + (snc.sql(Mixed_Q4).show))
      println("snc : Mixed_Q5 " + (snc.sql(Mixed_Q5).show))
    }

    /* --- Spark Job --- */
    spark.sql("CREATE SCHEMA T20")

    spark.sql("CREATE TABLE IF NOT EXISTS T20.TwentyTwenty(name String, " +
      "LastThreeMatchPerformance ARRAY<Double>, " +
      "Roll MAP<SMALLINT,STRING>, " +
      "Profile STRUCT<Matches:Long,Runs:Int,SR:Double,isPlaying:Boolean>) USING PARQUET")

    spark.sql("INSERT INTO T20.TwentyTwenty SELECT " +
      "'M S Dhoni',ARRAY(37,25,58),MAP(1,'WicketKeeper')," +
      "STRUCT(93,1487,127.09,true)")
    spark.sql("INSERT INTO T20.TwentyTwenty SELECT " +
      "'Yuvaraj Singh',ARRAY(68,72,21),MAP(2,'AllRounder')," +
      "STRUCT(58,1177,136.38,false)")
    spark.sql("INSERT INTO T20.TwentyTwenty SELECT " +
      "'Viral Kohli',ARRAY(52,102,23),MAP(3,'Batsmen'),STRUCT(65,2167,136.11,true)")
    spark.sql("INSERT INTO T20.TwentyTwenty SELECT " +
      "'Gautam Gambhir',ARRAY(35,48,74),MAP(3,'Batsmen')," +
      "STRUCT(37,932,119.02,false)")
    spark.sql("INSERT INTO T20.TwentyTwenty SELECT " +
      "'Rohit Sharma',ARRAY(0,56,44),MAP(3,'Batsmen'),STRUCT(90,2237,138.17,true)")
    spark.sql("INSERT INTO T20.TwentyTwenty SELECT " +
      "'Ravindra Jadeja',ARRAY(15,25,33),MAP(2,'AllRounder')," +
      "STRUCT(40,116,93.54,true)")
    spark.sql("INSERT INTO T20.TwentyTwenty SELECT " +
      "'Virendra Sehwag',ARRAY(5,45,39),MAP(3,'Batsmen')," +
      "STRUCT(19,394,145.39,false)")
    spark.sql("INSERT INTO T20.TwentyTwenty SELECT " +
      "'Hardik Pandya',ARRAY(27,14,19),MAP(2,'AllRounder')," +
      "STRUCT(35,271,153.10,true)")
    spark.sql("INSERT INTO T20.TwentyTwenty SELECT " +
      "'Suresh Raina',ARRAY(31,26,48),MAP(3,'Batsmen'),STRUCT(78,1605,134.87,false)")
    spark.sql("INSERT INTO T20.TwentyTwenty SELECT " +
      "'Harbhajan Singh',ARRAY(23,5,11),MAP(4,'Bowler'),STRUCT(28,108,124.13,false)")
    spark.sql("INSERT INTO T20.TwentyTwenty SELECT " +
      "'Ashish Nehra',ARRAY(2,1,5),MAP(4,'Bowler'),STRUCT(27,28,71.79,false)")
    spark.sql("INSERT INTO T20.TwentyTwenty SELECT " +
      "'Kuldeep Yadav',ARRAY(3,3,0),MAP(4,'Bowler'),STRUCT(17,20,100.0,true)")
    spark.sql("INSERT INTO T20.TwentyTwenty SELECT " +
      "'Parthiv Patel',ARRAY(29,18,9),MAP(1,'WicketKeeper')," +
      "STRUCT(2,36,112.50,false)")
    spark.sql("INSERT INTO T20.TwentyTwenty SELECT " +
      "'Ravichandran Ashwin',ARRAY(15,7,12),MAP(4,'Bowler')," +
      "STRUCT(46,123,106.95,true)")
    spark.sql("INSERT INTO T20.TwentyTwenty SELECT " +
      "'Irfan Pathan',ARRAY(17,23,18),MAP(2,'AllRounder')," +
      "STRUCT(24,172,119.44,false)")

    spark.sql(Mixed_Q1)
    spark.sql(Mixed_Q2)
    spark.sql(Mixed_Q3)
    spark.sql(Mixed_Q4)
    spark.sql(Mixed_Q5)

    if(printContent) {
      println("spark : Mixed_Q1 " + (spark.sql(Mixed_Q1).show))
      println("spark : Mixed_Q2 " + (spark.sql(Mixed_Q2).show))
      println("spark : Mixed_Q3 " + (spark.sql(Mixed_Q3).show))
      println("spark : Mixed_Q4 " + (spark.sql(Mixed_Q4).show))
      println("spark : Mixed_Q5 " + (spark.sql(Mixed_Q5).show))
    }



    /* --- Verification --- */

    // TODO Due to SNAP-2782 Below line is commented, Hydra Framework required changes.
    // SnappyTestUtils.assertQueryFullResultSet(snc, Mixed_Q1, "Mixed_Q1", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, Mixed_Q2, "Mixed_Q2", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, Mixed_Q3, "Mixed_Q3", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, Mixed_Q4, "Mixed_Q4", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, Mixed_Q5, "Mixed_Q5", "column", pw, sqlContext)

    /* --- Clean up --- */

    snc.sql("DROP TABLE IF EXISTS T20.TwentyTwenty")
    spark.sql("DROP TABLE IF EXISTS T20.TwentyTwenty")
    snc.sql("DROP SCHEMA IF EXISTS T20")
    spark.sql("DROP SCHEMA IF EXISTS T20")
  }
}
