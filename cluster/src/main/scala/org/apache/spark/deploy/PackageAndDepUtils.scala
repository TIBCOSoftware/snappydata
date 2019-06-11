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

package org.apache.spark.deploy

object GetJarsAndDependencies {

  val usage = s"Usage: GetJarsAndDependencies" +
      s" [--repos repositories] [--jarcache path] coordinates"

  def main(args: Array[String]) {
    if (args.length == 0) println(usage)
    val arglist = args.toList
    type OptionMap = Map[Symbol, String]

    def nextOption(map: OptionMap, list: List[String]): OptionMap = {
      def isSwitch(s: String) = (s(0) == '-')

      list match {
        case Nil => map
        case "--jarcache" :: value :: tail =>
          nextOption(map ++ Map('jarcache -> value), tail)
        case "--repos" :: value :: tail =>
          nextOption(map ++ Map('repos -> value), tail)
        case string :: opt2 :: tail if isSwitch(opt2) =>
          nextOption(map ++ Map('coordinates -> string), list.tail)
        case string :: Nil => nextOption(map ++ Map('coordinates -> string), list.tail)
        case option :: tail => println("Unknown option " + option)
          Map.empty
      }
    }

    val options = nextOption(Map(), arglist)

    val coordinates = options.getOrElse('coordinates, throw new IllegalArgumentException)
    val remoteRepos = options.get('repos)
    val ivyPath = options.get('jarcache)
    println(PackageAndDepUtils.resolveMavenCoordinates(coordinates, remoteRepos, ivyPath))
  }
}

object PackageAndDepUtils {
  def resolveMavenCoordinates(coordinates: String, remoteRepos: Option[String],
        ivyPath: Option[String], exclusions: Seq[String] = Nil, isTest: Boolean = false): String = {
    SparkSubmitUtils.resolveMavenCoordinates(coordinates, remoteRepos, ivyPath, exclusions, isTest)
  }
}
