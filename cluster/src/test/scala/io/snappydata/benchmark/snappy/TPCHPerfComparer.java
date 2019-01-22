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
package io.snappydata.benchmark.snappy;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TPCHPerfComparer {

  public static void main(String[] args) {
    //read given directory
    //create a list of map
    //iterate over all the actual run directories : decide the sequence
    //get the lead directory
    //get Average.out file
    //create a map of query Vs execution time
    //add this map into above list

    //for each query iterate over list of map
    // from map get the value
    //it the value is present
    //treat first value are base and divide subsequent values with this base value and plot values

    Path p = Paths.get(args[0]);
    final int maxDepth = 5;
    List<String> errorList = new ArrayList<String>();
    try {
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss") ;
      FileOutputStream reportOutputStream  = new FileOutputStream(new File(p.toString(), "ComparisonReport_"+dateFormat.format(new Date())+".txt"));
      PrintStream reportPrintStream = new PrintStream(reportOutputStream);

      Stream<Path> matches = Files.find(p, maxDepth, (path, attr) -> path.getFileName().toString().equals("1_Snappy_Average.out"));
      List<Path> files = matches.collect(Collectors.toList());

      Collections.sort(files, new Comparator<Path>() {
        public int compare(Path o1, Path o2) {
          try {
            return Files.getLastModifiedTime(o1).compareTo(Files.getLastModifiedTime(o2));
          } catch (IOException e) {
            e.printStackTrace();
          }
          return 0;
        }
      });

      Stream<Path> sortedPaths = files.stream();
      AtomicInteger atomicCount = new AtomicInteger(0);
      System.out.println("---------------------------------------------------------------------------------------");
      reportPrintStream.println("--------------------------------------------------------------------------------");
      reportPrintStream.println("                          Comparison Report                                     ");
      reportPrintStream.println("                        "+new Date()+"                                           ");
      reportPrintStream.println("--------------------------------------------------------------------------------");
      Stream<Map<Integer, Double>> averages = sortedPaths.map(path ->
      {
        int folderCount = atomicCount.incrementAndGet();
        Map<Integer, Double> perfMap = new HashMap<Integer, Double>();
        System.out.println("#"+folderCount + " : " + path.getParent().getParent().getParent());
        reportPrintStream.println("#"+folderCount + " : " + path.getParent().getParent().getParent());
        //reportPrintStream.println(atomicCount.incrementAndGet() + " : " + path.getParent().getParent().getParent());
        try {
          Files.lines(path).map(line->line.split(",")).forEach(element -> perfMap.put(Integer.parseInt(element[0]), Double.parseDouble(element[1])));
        } catch (Exception e) {
          e.printStackTrace();
        }
        return perfMap;
      });

      List<Map<Integer, Double>> averageList = averages.collect(Collectors.toList());
      System.out.println("---------------------------------------------------------------------------------------");
      reportPrintStream.println("--------------------------------------------------------------------------------");

      System.out.print("Query");
      reportPrintStream.print("Query");
      for(int j = 0 ; j < atomicCount.get(); j++) {
        System.out.print("     #" + j);
        reportPrintStream.print("     #" + j);
      }
      System.out.println();
      reportPrintStream.println();

      for(int i=1; i < 23; i++){
        System.out.print(i < 10 ? " 0" + i : " "+i);
        reportPrintStream.print(i < 10 ? " 0" + i : " "+i);
        System.out.print("     ");
        reportPrintStream.print("     ");
        int count = 0;
        Double firstValue = 0.0;
        int whichRevPerfDown = 0;
        for (Map<Integer, Double> singleMap : averageList) {
          if (count == 0) {
            if(singleMap.get(i) != null) {
              firstValue = singleMap.get(i);
              System.out.print("---");
              reportPrintStream.print("---");
              //System.out.print(firstValue);
              count++;
            }else{
              System.out.print("        ");
            }
          } else {
            if (singleMap.get(i) != null) {
              double secondValue = singleMap.get(i);
              double perf = (firstValue-secondValue)/secondValue;
              perf = perf > 0 ? 1 + perf : -1 + perf;
              if(perf > 0){
                System.out.print("    ");
                reportPrintStream.print("    ");
              }else{
                System.out.print("   ");
                reportPrintStream.print("   ");
                if(perf < -1.10){
                  errorList.add("For Query "+ i + ", It is observed that revision #" + whichRevPerfDown +"'s performance degraded by "+ perf);
                }
              }
              System.out.printf("%.2f",perf);
              reportPrintStream.printf("%.2f",perf);
            }
          }
          whichRevPerfDown++;
        }
        System.out.println("");
        reportPrintStream.println("");
      }
      reportPrintStream.println("------------------------------------------------------------------------------------");
      for(String error : errorList){
        System.out.println(error);
        reportPrintStream.println(error);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    assert errorList.isEmpty(): "Performance degradation is observed for TPCH queries. Please have a look at ComparisonReport.txt";
  }
}

