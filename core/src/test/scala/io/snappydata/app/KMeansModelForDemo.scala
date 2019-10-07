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
package io.snappydata.app

import java.io.File

import scala.collection.mutable
import scala.io.Source

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

object KMeansModelForDemo extends App {

  // val Array(zip, interval, files @ _*) = args
  val Array(zip, interval, inp_dir, fileStartsWith, debug) = args
  val DEBUG = debug.toBoolean

  def dprintf(printables: String*) = {
    if (DEBUG) {
      printables.foreach(println(_))
    }
  }
  def dprintf2(printables: String*) = {
    val sb = new mutable.StringBuilder()
    if (DEBUG) {
      printables.foreach(x => sb.append(x + ","))
    }
    println(sb.toString().substring(0, sb.toString().length - 2))
  }
  dprintf("zip = " + zip, "inp_dir = " + inp_dir, "fileStartsWith = " + fileStartsWith)

  def sortFunc(f1: File, f2: File): Boolean = {
    f1.lastModified() < f2.lastModified()
  }
  //val allfiles = new java.io.File(inp_dir).listFiles.filter(_.getName.startsWith(fileStartsWith)).sortWith(sortFunc)
  //val files = allfiles.slice(allfiles.length-1, allfiles.length)
  //dprintf("file = " + files(0), files.length.toString)

  val allfiles = new java.io.File(inp_dir).listFiles.filter(_.getName.startsWith(fileStartsWith))
  dprintf("length of allfiles = " + allfiles.length)
  dprintf("file = " + allfiles(0))

  var dataVector = Array[Double]()
  var sums = new mutable.HashMap[String, Double]()
  var lastTS = 0L
  //for (f <- files) {
  for (f <- allfiles) {
    dprintf(f.getAbsolutePath)
    val src = Source.fromFile(f).getLines()
    for (l <- src) {
      val Array(ts, v, vtype, pgId, hhId, hId, area, zip) = l.split(",")
      //dprintf2(ts, v, vtype, pgId, hhId, hId, area, zip)
      //println
      val key = hhId+hId
      val tmpv = v.toDouble
      if (vtype.toInt == 1) {
        val old = sums.getOrElse(key, 0.0)
        // dprintf2(key, tmpv.toString)
        // println()
        sums.put(key, tmpv + old)
      }
      if (lastTS == 0) {
        lastTS = ts.toLong
      }
      else if (ts.toLong - lastTS == interval.toInt) {
        lastTS += interval.toInt
        dataVector = dataVector ++ sums.values.toVector
        sums.clear()
      }
    }
    if (sums.nonEmpty) {
      dataVector = dataVector ++ sums.values.toVector
    }
  }

  dprintf("Size = " + dataVector.size)
  // dataVector.foreach(println(_))

  val conf = new org.apache.spark.SparkConf().setAppName("KMeansDemo")
    .set("spark.logConf", "true")
  val setMaster: String = "local[6]"
  if (setMaster != null) {
    //"local-cluster[3,2,1024]"
    conf.setMaster(setMaster)
  }

  var list: mutable.MutableList[org.apache.spark.mllib.linalg.Vector] = mutable.MutableList()
  val size = dataVector.length
  val sizeOfEach = size / 6
  dprintf("Size of each = " + sizeOfEach)
  for ( x <- 0 until 6) {
    //println("x = " + x)
    if (x < 5) {
      //println("subarr.length = " + ", start = " + x * sizeOfEach + ", end = " + (x * sizeOfEach + sizeOfEach))
      val subarr = java.util.Arrays.copyOfRange(dataVector, x * sizeOfEach, (x * sizeOfEach + sizeOfEach))
      list += Vectors.dense(subarr)
    }
    else {
      //println("subarr.length = " + ", start = " + x * sizeOfEach + ", end = " + (size - 1))
      if (size - (x * sizeOfEach) >= sizeOfEach) {
        val subarr = java.util.Arrays.copyOfRange(dataVector, x * sizeOfEach, (x * sizeOfEach) + sizeOfEach)
        list += Vectors.dense(subarr)
      }
    }
  }

  println("list Size = " + list.size)

  var i = 0
  list.foreach(l => {
    if (i % 10 == 0) {
      println()
    }
    print(l + ",")
  })
  println()
//  //===============
//  def unique[A](ls: List[A]) = {
//    def loop(set: Set[A], ls: List[A]): List[A] = ls match {
//      case hd :: tail if set contains hd => loop(set, tail)
//      case hd :: tail => hd :: loop(set + hd, tail)
//      case Nil => Nil
//    }
//
//    loop(Set(), ls)
//  }
////  implicit def listToSyntax[A](ls: List[A]) = new {
////    def unique = unique(ls)
////  }
//  //===============


  var set: mutable.HashSet[Double] = new mutable.HashSet[Double]()
  dataVector.foreach(d => set += d)

  println("Unique elements = " + set.size)
  val sc = new org.apache.spark.SparkContext(conf)
  val avgrdd = sc.parallelize(list).cache()
  val numClusters = 4
  val numIterations = 5
  val clusters = KMeans.train(avgrdd, numClusters, numIterations)
  println("Cluster centers = " + clusters.clusterCenters + " size = " + clusters.clusterCenters.size)

  clusters.clusterCenters.foreach( c => println("size of c = " + c.size + ", c = " + c))
  val WSSSE = clusters.computeCost(avgrdd)
  println("Within Set Sum of Squared Errors = " + WSSSE)


  val mykmean = new KMeans()
  mykmean.run(avgrdd)
//  val testDataPoint = Vectors.dense(60587.17999999997,62284.62999999998,59971.85999999997,
//    61965.85000000006,56818.17000000003,55830.17999999999,63085.09000000003,55755.98999999998,54191.560000000005,40930.490000000005,
//    40168.59000000001,40633.07499999997,39620.36500000001,39273.83999999998,41702.83499999998,42035.08500000002)
  val testDataPoint = Vectors.dense(19220.260000000002,17937.265000000007,16721.97000000001,
  19020.275,17626.835,20007.775,19587.850000000006,21631.055,19211.36999999999,19758.660000000003,21287.215000000004,
  19470.844999999994,18576.934999999998,19790.515000000014,19694.05,21441.40000000001)
  println("Closest cluster index for the given vector = " + clusters.predict(testDataPoint))

//  dataVector.foreach( d => {
  var list2: mutable.MutableList[org.apache.spark.mllib.linalg.Vector] = mutable.MutableList()
  val size2 = dataVector.length
  val numOfVectors = size2 / 5
  // prepare vectors of length 5
  for ( i <- 0 until numOfVectors ) {
    list2 += Vectors.dense (dataVector(i*5), dataVector(i*5 + 1), dataVector(i*5 + 2), dataVector(i*5 + 3), dataVector(i*5 + 4))
  }

  val avg2rdd = sc.parallelize(list2).cache()

  val clusters2 = KMeans.train(avg2rdd, numClusters, numIterations)
  println("Cluster 2 centers = " + clusters2.clusterCenters + " size = " + clusters2.clusterCenters.size)

  clusters2.clusterCenters.foreach( c => println("cluster center c = " + c))
  val WSSSE2 = clusters2.computeCost(avg2rdd)
  println("Within Set Sum of Squared Errors = " + WSSSE2)

//  (5 to 40 by 5).par.map( k => {
//    k.getClass.getSimpleName
//  })
}
