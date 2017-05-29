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
import hydra.FileUtil;
import hydra.Log;
import io.snappydata.hydra.cluster.SnappyBB;
import io.snappydata.hydra.cluster.SnappyTest;
import util.TestException;

/**
 * Created by kishor on 12/5/17.
 */
public class TPCHPerfComparerTest extends SnappyTest {

  public static void HydraTask_CompareQueryPerformance() {

    String primaryLeadDir = null;
    Object[] tmpArr = null;
    String leadPort = null;
    tmpArr = getPrimaryLeadVM(cycleLeadVMTarget);
    List<ClientVmInfo> vmList;
    vmList = (List<ClientVmInfo>)(tmpArr[0]);
    Set<String> myDirList = new LinkedHashSet<String>();
    myDirList = getFileContents("logDir_", myDirList);
    for (int i = 0; i < vmList.size(); i++) {
      ClientVmInfo targetVm = vmList.get(i);
      String clientName = targetVm.getClientName();
      for (String vmDir : myDirList) {
        if (vmDir.contains(clientName)) {
          primaryLeadDir = vmDir;
        }
      }
    }

    try {
      Log.getLogWriter().info("KBKBKB : " + primaryLeadDir);

      FileWriter writer = new FileWriter("SnappyVsSpark.out");
      BufferedWriter buffer = new BufferedWriter(writer);
      buffer.write("Query   Snappy   Spark   PercentageDiff");

      FileInputStream snappyFS = new FileInputStream(primaryLeadDir + File.separator + "1_Snappy_Average.out");
      FileInputStream sparkFS = new FileInputStream(primaryLeadDir + File.separator + "1_Spark_Average.out");
      BufferedReader snappyBR = new BufferedReader(new InputStreamReader(snappyFS));
      BufferedReader sparkBR = new BufferedReader(new InputStreamReader(sparkFS));
      String snappyLine = null;
      String sparkLine = null;
      String line1 = snappyBR.readLine();
      String line2 = sparkBR.readLine();

      boolean areEqual = true;
      int lineNum = 1;

      String degradedPerfs = "";

      while (line1 != null || line2 != null) {
        String[] snappyQueryPerformance = line1.split(",");
        String[] sparkQueryPerformance = line2.split(",");
        if (snappyQueryPerformance[0].equals(sparkQueryPerformance[0])) {
          Double snappyValue = new Double(snappyQueryPerformance[1]);
          Double sparkValue = new Double(sparkQueryPerformance[1]);

          double diff = sparkValue - snappyValue;
          double percentageDiff = (diff / sparkValue) * 100;

          buffer.write("\n  " + snappyQueryPerformance[0] + "         " + snappyQueryPerformance[1] + "        " + sparkQueryPerformance[1] + "              " + percentageDiff);

          if (percentageDiff < -5) {
            degradedPerfs +=  "For query " + snappyQueryPerformance[0] + "Snappy Performance is degraded by " + percentageDiff + "% \n";
          }
        } else {
          new TestException("Query execution is not aligned");
        }
        line1 = snappyBR.readLine();
        line2 = sparkBR.readLine();
        lineNum++;
      }
      if(!degradedPerfs.isEmpty()){
        new TestException("Performance degradation observed for below queries " + degradedPerfs);
      }
      buffer.close();
      snappyBR.close();
      sparkBR.close();
    } catch (IOException e) {
      throw new TestException("Query Performance files are not available");
    }
  }
}
