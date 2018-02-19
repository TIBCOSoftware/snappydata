package io.snappydata.hydra.tpch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import hydra.ClientVmInfo;
import io.snappydata.hydra.cluster.SnappyTest;
import util.TestException;

public class TPCHPerfComparerTest extends SnappyTest {

  @SuppressWarnings("unused")
  public static void HydraTask_CompareQueryPerformance() {

    String primaryLeadDir = null;
    Object[] tmpArr;
    tmpArr = getPrimaryLeadVM(cycleLeadVMTarget);
    List<ClientVmInfo> vmList;
    // noinspection unchecked
    vmList = (List<ClientVmInfo>)(tmpArr[0]);
    Set<String> myDirList = new LinkedHashSet<>();
    myDirList = getFileContents("logDir_", myDirList);
    for (ClientVmInfo targetVm : vmList) {
      String clientName = targetVm.getClientName();
      for (String vmDir : myDirList) {
        if (vmDir.contains(clientName)) {
          primaryLeadDir = vmDir;
        }
      }
    }

    try {
      FileWriter writer = new FileWriter("SnappyVsSpark.out");
      BufferedWriter buffer = new BufferedWriter(writer);
      buffer.write("Query   Snappy   Spark   PercentageDiff");

      FileInputStream snappyFS = new FileInputStream(primaryLeadDir +
          File.separator + "1_Snappy_Average.out");
      FileInputStream sparkFS = new FileInputStream(primaryLeadDir +
          File.separator + "1_Spark_Average.out");
      BufferedReader snappyBR = new BufferedReader(new InputStreamReader(snappyFS));
      BufferedReader sparkBR = new BufferedReader(new InputStreamReader(sparkFS));
      String line1 = snappyBR.readLine();
      String line2 = sparkBR.readLine();

      int lineNum = 1;

      String degradedPerfs = "";

      while (line1 != null || line2 != null) {
        String[] snappyQueryPerformance = line1 != null ? line1.split(",") : new String[0];
        String[] sparkQueryPerformance = line2 != null ? line2.split(",") : new String[0];
        if (snappyQueryPerformance.length == sparkQueryPerformance.length &&
            snappyQueryPerformance[0].equals(sparkQueryPerformance[0])) {
          Double snappyValue = new Double(snappyQueryPerformance[1]);
          Double sparkValue = new Double(sparkQueryPerformance[1]);

          double diff = sparkValue - snappyValue;
          double percentageDiff = (diff / sparkValue) * 100;

          buffer.write("\n  " + snappyQueryPerformance[0] + "         " +
              snappyQueryPerformance[1] + "        " + sparkQueryPerformance[1] +
              "              " + percentageDiff);

          if (percentageDiff < -5) {
            degradedPerfs += "For query " + snappyQueryPerformance[0] +
                "Snappy Performance is degraded by " + percentageDiff + "% \n";
          }
        } else {
          throw new TestException("Query execution is not aligned");
        }
        line1 = snappyBR.readLine();
        line2 = sparkBR.readLine();
        lineNum++;
      }
      if (!degradedPerfs.isEmpty()) {
        throw new TestException("Performance degradation observed for below queries \n" + degradedPerfs);
      }
      buffer.close();
      snappyBR.close();
      sparkBR.close();
    } catch (IOException e) {
      throw new TestException("Query Performance files are not available");
    }
  }
}
