/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Changes for SnappyData data platform.
 *
 * Portions Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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
package io.snappydata.test.dunit.standalone;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.gemstone.gemfire.internal.FileUtil;
import com.gemstone.gemfire.internal.shared.NativeCalls;
import io.snappydata.test.dunit.RemoteDUnitVMIF;
import io.snappydata.test.dunit.logging.log4j.ConfigLocator;
import org.apache.commons.io.FileUtils;

/**
 * @author dsmith
 *
 */
public class ProcessManager {
  
  private int namingPort;
  private Map<Integer, ProcessHolder> processes = new HashMap<Integer, ProcessHolder>();
  private File log4jConfig;
  private int pendingVMs;
  private Registry registry;

  public ProcessManager(int namingPort, Registry registry) {
    this.namingPort = namingPort;
    this.registry = registry;
  }
  
  public void launchVMs() throws IOException, NotBoundException {
    log4jConfig = ConfigLocator.findConfigInWorkingDirectory();
  }

  public synchronized void launchVM(int vmNum) throws IOException {
    if(processes.containsKey(vmNum)) {
      throw new IllegalStateException("VM " + vmNum + " is already running.");
    }
    
    String[] cmd = buildJavaCommand(vmNum, namingPort);
    System.out.println("Executing " + Arrays.asList(cmd));
    File workingDir = getVMDir(vmNum, true);
    try {
      FileUtil.delete(workingDir);
    } catch(IOException e) {
      //This delete is occasionally failing on some platforms, maybe due to a lingering
      //process. Allow the process to be launched anyway.
      System.err.println("Unable to delete " + workingDir + ". Currently contains " 
                          + Arrays.asList(workingDir.list()));
    }
    workingDir.mkdirs();
    if (log4jConfig != null) {
      FileUtils.copyFileToDirectory(log4jConfig, workingDir);
    }
    
    //TODO - delete directory contents, preferably with commons io FileUtils
    Process process = Runtime.getRuntime().exec(cmd, null, workingDir);
    pendingVMs++;
    ProcessHolder holder = new ProcessHolder(process,
        workingDir.getAbsoluteFile());
    processes.put(vmNum, holder);
    linkStreams(vmNum, holder, process.getErrorStream(), System.err);
    linkStreams(vmNum, holder, process.getInputStream(), System.out);
  }

  public File getVMDir(int vmNum, boolean launch) {
    if (launch) {
      return new File(DUnitLauncher.DUNIT_DIR, "vm" + vmNum +
          '_' + NativeCalls.getInstance().getProcessId());
    } else {
      ProcessHolder holder = this.processes.get(vmNum);
      if (holder != null) {
        return holder.getWorkingDir();
      } else {
        throw new IllegalArgumentException("No VM " + vmNum + " found.");
      }
    }
  }

  public synchronized void killVMs() {
    for(ProcessHolder process : processes.values()) {
      if(process != null) {
        //TODO - stop it gracefully? Why bother
        process.kill();
      }
    }
  }
  
  public synchronized void bounce(int vmNum) {
    if(!processes.containsKey(vmNum)) {
      throw new IllegalStateException("No such process " + vmNum);
    }
    try {
      ProcessHolder holder = processes.remove(vmNum);
      holder.kill();
      holder.getProcess().waitFor();
      launchVM(vmNum);
    } catch (InterruptedException | IOException e) {
      throw new RuntimeException("Unable to restart VM " + vmNum, e);
    }
  }
   
  private void linkStreams(final int vmNum, final ProcessHolder holder, final InputStream in, final PrintStream out) {
    Thread ioTransport = new Thread() {
      public void run() {
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String vmName = (vmNum==-2)? "[locator]" : "[vm_"+vmNum+"]";
        try {
          String line = reader.readLine();
          while(line != null) {
            out.print(vmName);
            out.println(line);
            line = reader.readLine();
          }
        } catch(Exception e) {
          if(!holder.isKilled()) {
            out.println("Error transporting IO from child process");
            e.printStackTrace(out);
          }
        }
      }
    };

    ioTransport.setDaemon(true);
    ioTransport.start();
  }

  private String[] buildJavaCommand(int vmNum, int namingPort) {
    String cmd = System.getProperty( "java.home" ) + File.separator + "bin" + File.separator + "java";
    String classPath = System.getProperty("java.class.path");
    //String tmpDir = System.getProperty("java.io.tmpdir");
    String agent = getAgentString();
    // limit netty buffer arenas and sizes to avoid occasional OOMEs with 1g heap
    int pageSize = 8192;
    int maxOrder = 10; // total size of each chunk will be "pageSize << maxOrder" i.e. 8MB
    int numArenas = Math.min(8, Runtime.getRuntime().availableProcessors() * 2);
    return new String[] {
      cmd, "-classpath", classPath,
      "-D" + DUnitLauncher.RMI_PORT_PARAM + "=" + namingPort,
      "-D" + DUnitLauncher.VM_NUM_PARAM + "=" + vmNum,
      "-D" + DUnitLauncher.WORKSPACE_DIR_PARAM + "=" + new File(".").getAbsolutePath(),
      "-DlogLevel=" + DUnitLauncher.LOG_LEVEL,
      "-Dgemfire.log-level=" + DUnitLauncher.LOG_LEVEL,
      "-DsecurityLogLevel=" + DUnitLauncher.SECURITY_LOG_LEVEL,
      "-Dgemfire.security-log-level=" + DUnitLauncher.SECURITY_LOG_LEVEL,
      "-Djava.library.path=" + System.getProperty("java.library.path"),
      "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n",
      "-XX:+HeapDumpOnOutOfMemoryError",
      "-Xmx1g",
      "-Xms1g",
      "-XX:+UseParNewGC",
      "-XX:+UseConcMarkSweepGC",
      "-XX:CMSInitiatingOccupancyFraction=50",
      "-XX:+CMSClassUnloadingEnabled",
      "-Dgemfire.DEFAULT_MAX_OPLOG_SIZE=10",
      "-Dgemfire.disallowMcastDefaults=true",
      "-Djava.net.preferIPv4Stack=true",
      "-Dsun.rmi.dgc.client.gcInterval=600000 ",
      "-Dsun.rmi.dgc.server.gcInterval=600000",
      "-Dsun.rmi.transport.tcp.handshakeTimeout=3600000",
      "-Dspark.sql.codegen.cacheSize=300",
      "-Dspark.ui.retainedStages=500",
      "-Dspark.ui.retainedJobs=500",
      "-Dspark.sql.ui.retainedExecutions=500",
      "-Dio.netty.allocator.pageSize=" + pageSize,
      "-Dio.netty.allocator.maxOrder=" + maxOrder,
      "-Dio.netty.allocator.numHeapArenas=" + numArenas,
      "-Dio.netty.allocator.numDirectArenas=" + numArenas,
      "-ea",
      agent,
      ChildVM.class.getName()
    };
  }

  /**
   * Get the java agent passed to this process and pass it to the child VMs.
   * This was added to support jacoco code coverage reports
   */
  private String getAgentString() {
    RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
    if (runtimeBean != null) {
      for(String arg: runtimeBean.getInputArguments()) {
        if(arg.contains("-javaagent:")) {
          //HACK for gradle bug  GRADLE-2859. Jacoco is passing a relative path
          //That won't work when we pass this to dunit VMs in a different 
          //directory
          arg = arg.replace("-javaagent:..", "-javaagent:" + System.getProperty("user.dir") + File.separator + "..");
          arg = arg.replace("destfile=..", "destfile=" + System.getProperty("user.dir") + File.separator + "..");
          return arg;
        }
      }
    }
    
    return "-DdummyArg=true";
  }

  synchronized void signalVMReady() {
    pendingVMs--;
    this.notifyAll();
  }
  
  public synchronized boolean waitForVMs(long timeout) throws InterruptedException {
    long end = System.currentTimeMillis() + timeout;
    while(pendingVMs > 0) {
      long remaining = end - System.currentTimeMillis();
      if(remaining <= 0) {
        return false;
      }
      this.wait(remaining);
    }
    
    return true;
  }
  
  private static class ProcessHolder {
    private final Process process;
    private final File workingDir;
    private volatile boolean killed = false;

    public ProcessHolder(Process process, File workingDir) {
      this.process = process;
      this.workingDir = workingDir;
    }

    public void kill() {
      this.killed = true;
      process.destroy();
      
    }

    public Process getProcess() {
      return process;
    }

    public File getWorkingDir() {
      return this.workingDir;
    }

    public boolean isKilled() {
      return killed;
    }
  }

  public RemoteDUnitVMIF getStub(int i) throws AccessException, RemoteException, NotBoundException, InterruptedException {
    waitForVMs(DUnitLauncher.STARTUP_TIMEOUT);
    return (RemoteDUnitVMIF) registry.lookup("vm" + i);
  }
}
