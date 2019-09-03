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
 * Portions Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.nio.channels.FileChannel;
import java.rmi.AccessException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Semaphore;

import com.gemstone.gemfire.distributed.Locator;
import io.snappydata.test.batterytest.greplogs.ExpectedStrings;
import io.snappydata.test.batterytest.greplogs.LogConsumer;
import io.snappydata.test.dunit.AvailablePortHelper;
import io.snappydata.test.dunit.BounceResult;
import io.snappydata.test.dunit.DUnitEnv;
import io.snappydata.test.dunit.Host;
import io.snappydata.test.dunit.RemoteDUnitVMIF;
import io.snappydata.test.dunit.SerializableCallable;
import io.snappydata.test.dunit.VM;
import io.snappydata.test.hydra.MethExecutorResult;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.junit.Assert;

/**
 * A class to build a fake test configuration and launch some DUnit VMS.
 * 
 * For use within eclipse. This class completely skips hydra and just starts
 * some vms directly, creating a fake test configuration
 * 
 * Also, it's a good idea to set your working directory, because the test code
 * a lot of files that it leaves around.
 * 
 * @author dsmith
 *
 */
public class DUnitLauncher {

  static int locatorPort;
  static ProcessManager processManager;

  private static MasterRemote masterRemote;
  private static final Map<Object, Object> blackboard = new HashMap<>();
  private static final Semaphore sharedLock = new Semaphore(1);

  private static final int NUM_VMS = Integer.getInteger(
      "gemfire.DUnitLauncher.NUM_VMS", 4);
  private static final int DEBUGGING_VM_NUM = -1;
  private static final int LOCATOR_VM_NUM = -2;

  static final long STARTUP_TIMEOUT = 30 * 1000;
  private static final String SUSPECT_FILENAME = "dunit_suspect.log";
  private static File DUNIT_SUSPECT_FILE;

  public static final String DUNIT_DIR = "dunit";
  public static final String LOG_LEVEL = System.getProperty("logLevel", "config");
  public static final String SECURITY_LOG_LEVEL = System.getProperty(
      "securityLogLevel", "config");
  public static final String WORKSPACE_DIR_PARAM = "WORKSPACE_DIR";
  public static final boolean LOCATOR_LOG_TO_DISK = Boolean.getBoolean("locatorLogToDisk");

  static final String MASTER_PARAM = "DUNIT_MASTER";
  static final String RMI_PORT_PARAM = "gemfire.DUnitLauncher.RMI_PORT";
  public static final String VM_NUM_PARAM = "gemfire.DUnitLauncher.VM_NUM";

  private static final String LAUNCHED_PROPERTY = "gemfire.DUnitLauncher.LAUNCHED";

  private DUnitLauncher() {
  }
  
  private static boolean isHydra() {
    try {
      //TODO - this is hacky way to test for a hydra environment - see
      //if there is registered test configuration object.
      Class<?> clazz = Class.forName("hydra.TestConfig");
      Method getInstance = clazz.getMethod("getInstance", new Class[0]);
      getInstance.invoke(null);
      return true;
    } catch (Exception e) {
      return false;
    }
  }
  /**
   * Launch DUnit. If the unit test was launched through
   * the hydra framework, leave the test alone.
   */
  public static void launchIfNeeded() {
    if(System.getProperties().contains(VM_NUM_PARAM)) {
      //we're a dunit child vm, do nothing.
      return;
    }

    if(!isHydra() &&!isLaunched()) {
      try {
        launch();
      } catch (Exception e) {
        throw new RuntimeException("Unable to launch dunit VMS", e);
      }
    }
  }
  
  /**
   * Test it see if the eclise dunit environment is launched.
   */
  public static boolean isLaunched() {
    return Boolean.getBoolean(LAUNCHED_PROPERTY);
  }
  
  public static String getLocatorString() {
    return "localhost[" + locatorPort + "]";
  }

  public static int getLocator() {
    return locatorPort;
  }

  private static void launch() throws URISyntaxException, AlreadyBoundException, IOException, InterruptedException, NotBoundException  {
    DUNIT_SUSPECT_FILE = new File(SUSPECT_FILENAME);
    DUNIT_SUSPECT_FILE.delete();
    DUNIT_SUSPECT_FILE.deleteOnExit();
    
    locatorPort = AvailablePortHelper.getRandomAvailableTCPPort();

    //create an RMI registry and add an object to share our tests config
    int namingPort = AvailablePortHelper.getRandomAvailableTCPPort();
    Registry registry = LocateRegistry.createRegistry(namingPort);

    processManager = new ProcessManager(namingPort, registry);
    Master master = new Master(registry, processManager);
    registry.bind(MASTER_PARAM, master);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        processManager.killVMs();
      }
    });
    
    //Create a VM for the locator
    processManager.launchVM(LOCATOR_VM_NUM);
    
    //Launch an initial set of VMs
    for(int i=0; i < NUM_VMS; i++) {
      processManager.launchVM(i);
    }
    
    //wait for the VMS to start up
    if(!processManager.waitForVMs(STARTUP_TIMEOUT)) {
      throw new RuntimeException("VMs did not start up with 30 seconds");
    }
    
    //populate the Host class with our stubs. The tests use this host class
    DUnitHost host = new DUnitHost("localhost", processManager);
    host.init(registry, NUM_VMS);

    init(master);
  }
  
  public static Properties getDistributedSystemProperties() {
    Properties p = new Properties();
    p.setProperty("locators", getLocatorString());
    p.setProperty("mcast-port", "0");
    // Below two properties are not available in GemFireXD yet
//    p.setProperty("enable-cluster-configuration", "false");
//    p.setProperty("use-cluster-configuration", "false");
    p.setProperty("log-level", LOG_LEVEL);
    p.setProperty("security-log-level", SECURITY_LOG_LEVEL);
    return p;
  }

  /**
   * Add an appender to Log4j which sends all INFO+ messages to a separate file
   * which will be used later to scan for suspect strings.  The pattern of the
   * messages conforms to the original log format so that hydra will be able
   * to parse them.
   */
  private static void addSuspectFileAppender(final String workspaceDir) {
    final String suspectFilename = new File(workspaceDir, SUSPECT_FILENAME).getAbsolutePath();

    final Logger logger = LogManager.getLogger(Host.BASE_LOGGER_NAME);
    logger.setLevel(Level.INFO);

    final PatternLayout layout = new PatternLayout(
        "[%level{lowerCase=true} %date{yyyy/MM/dd HH:mm:ss.SSS z} <%thread> tid=%tid] %message%n%throwable%n");

    try {
      final FileAppender fileAppender = new FileAppender(layout, suspectFilename, true);
      logger.addAppender(fileAppender);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  private static void startLocator(Registry registry) throws IOException, NotBoundException {
    RemoteDUnitVMIF remote = (RemoteDUnitVMIF) registry.lookup("vm" + LOCATOR_VM_NUM);
    final File locatorLogFile =
        LOCATOR_LOG_TO_DISK ? new File("locator-" + locatorPort + ".log") : new File(""); 
    MethExecutorResult result = remote.executeMethodOnObject(new SerializableCallable() {
      public Object call() throws IOException {
        Properties p = getDistributedSystemProperties();
        // I never want this locator to end up starting a jmx manager
        // since it is part of the unit test framework
        p.setProperty("jmx-manager", "false");
        //Disable the shared configuration on this locator.
        //Shared configuration tests create their own locator
//        p.setProperty("enable-cluster-configuration", "false");
        Locator.startLocatorAndDS(locatorPort, locatorLogFile, p);
        return null;
      }
    }, "call");
    if(result.getException() != null) {
      RuntimeException ex = new RuntimeException("Failed to start locator", result.getException());
      ex.printStackTrace();
      throw ex;
    }
  }

  public static void init(MasterRemote master) {
    masterRemote = master;
    DUnitEnv.set(new StandAloneDUnitEnv(master));
    //fake out tests that are using a bunch of hydra stuff
    String workspaceDir = System.getProperty(DUnitLauncher.WORKSPACE_DIR_PARAM) ;
    workspaceDir = workspaceDir == null ? new File(".").getAbsolutePath() : workspaceDir;
    
    addSuspectFileAppender(workspaceDir);
    
    //Free off heap memory when disconnecting from the distributed system
    System.setProperty("gemfire.free-off-heap-memory", "true");
    
    //indicate that this CM is controlled by the eclipse dunit.
    System.setProperty(LAUNCHED_PROPERTY, "true");
  }

  public static MasterRemote getMaster() {
    return masterRemote;
  }

  public static void checkForSuspectStrings() {
    if (isLaunched()) {
      final boolean skipLogMsgs = ExpectedStrings.skipLogMsgs("dunit");
      final List<?> expectedStrings = ExpectedStrings.create("dunit");
      final LogConsumer logConsumer = new LogConsumer(skipLogMsgs, expectedStrings, "log4j", 5);

      final StringBuilder suspectStringBuilder = new StringBuilder();

      BufferedReader buffReader;
      FileChannel fileChannel;
      try {
        fileChannel = new FileOutputStream(DUNIT_SUSPECT_FILE, true).getChannel();
        buffReader = new BufferedReader(new FileReader(DUNIT_SUSPECT_FILE));
      } catch (FileNotFoundException e) {
        System.err.println("Could not find the suspect string output file: " + e);
        return;
      }
      try {
        String line;
        try {
          while ((line = buffReader.readLine()) != null) {
            final StringBuilder builder = logConsumer.consume(line);
            if (builder != null) {
              suspectStringBuilder.append(builder);
            }
          }
        } catch (IOException e) {
          System.err.println("Could not read the suspect string output file: " + e);
        }
        
        try {
          fileChannel.truncate(0);
        } catch (IOException e) {
          System.err.println("Could not truncate the suspect string output file: " + e);
        }
        
      } finally {
        try {
          buffReader.close();
          fileChannel.close();
        } catch (IOException e) {
          System.err.println("Could not close the suspect string output file: " + e);
        }
      }

      if (suspectStringBuilder.length() != 0) {
        System.err.println("Suspicious strings were written to the log during this run.\n"
            + "Fix the strings or use DistributedTestBase.addExpectedException to ignore.\n"
            + suspectStringBuilder);
        
        Assert.fail("Suspicious strings were written to the log during this run.\n"
            + "Fix the strings or use DistributedTestBase.addExpectedException to ignore.\n"
            + suspectStringBuilder);
      }
    }
  }

  public interface MasterRemote extends Remote {
    public int getLocatorPort() throws RemoteException;
    public void signalVMReady() throws RemoteException;
    public void ping() throws RemoteException;
    public BounceResult bounce(int pid) throws RemoteException;
    public Object get(Object key) throws RemoteException;
    public boolean put(Object key, Object value) throws RemoteException;
    public boolean remove(Object key) throws RemoteException;
    public int addAndGet(Object key, int delta,
        int defaultValue) throws RemoteException;
    public Map<Object, Object> getMapCopy() throws RemoteException;
    public void acquireSharedLock() throws RemoteException;
    public void releaseSharedLock() throws RemoteException;
  }

  public static class Master extends UnicastRemoteObject implements MasterRemote {
    private static final long serialVersionUID = 1178600200232603119L;
    
    private final Registry registry;
    private final ProcessManager processManager;


    public Master(Registry registry, ProcessManager processManager) throws RemoteException {
      this.processManager = processManager;
      this.registry = registry;
    }

    @Override
    public int getLocatorPort()  throws RemoteException{
      return locatorPort;
    }

    @Override
    public synchronized void signalVMReady() {
      processManager.signalVMReady();
    }

    @Override
    public void ping() {
      //do nothing
    }

    @Override
    public BounceResult bounce(int pid) {
      processManager.bounce(pid);
      
      try {
        if(!processManager.waitForVMs(STARTUP_TIMEOUT)) {
          throw new RuntimeException("VMs did not start up with 30 seconds");
        }
        RemoteDUnitVMIF remote = (RemoteDUnitVMIF) registry.lookup("vm" + pid);
        return new BounceResult(pid, remote);
      } catch (RemoteException | NotBoundException e) {
        throw new RuntimeException("could not lookup name", e);
      } catch (InterruptedException e) {
        throw new RuntimeException("Failed waiting for VM", e);
      }
    }

    @Override
    public Object get(Object key) {
      synchronized (blackboard) {
        return blackboard.get(key);
      }
    }

    @Override
    public boolean put(Object key, Object value) {
      synchronized (blackboard) {
        return (blackboard.put(key, value) == null);
      }
    }

    @Override
    public boolean remove(Object key) {
      synchronized (blackboard) {
        return (blackboard.remove(key) != null);
      }
    }

    @Override
    public int addAndGet(Object key, int delta, int defaultValue) {
      synchronized (blackboard) {
        Object current = blackboard.get(key);
        if (current == null) {
          blackboard.put(key, defaultValue);
          return defaultValue;
        } else {
          int newValue = ((Integer)current) + delta;
          blackboard.put(key, newValue);
          return newValue;
        }
      }
    }

    @Override
    public Map<Object, Object> getMapCopy() {
      synchronized (blackboard) {
        return new HashMap<>(blackboard);
      }
    }

    @Override
    public void acquireSharedLock() {
      try {
        sharedLock.acquire();
      } catch (InterruptedException ie) {
        throw new RuntimeException("Failed waiting for shared lock", ie);
      }
    }

    @Override
    public void releaseSharedLock() {
      sharedLock.release();
    }
  }

  private static class DUnitHost extends Host {
    private static final long serialVersionUID = -8034165624503666383L;
    
    private transient final VM debuggingVM;

    private transient ProcessManager processManager;
    
    public DUnitHost(String hostName, ProcessManager processManager) throws RemoteException {
      super(hostName);
      this.debuggingVM = new VM(this, -1, new RemoteDUnitVM());
      this.processManager = processManager;
    }
    
    public void init(Registry registry, int numVMs) throws AccessException, RemoteException, NotBoundException, InterruptedException {
      for(int i = 0; i < numVMs; i++) {
        RemoteDUnitVMIF remote = processManager.getStub(i);
        addVM(i, remote);
      }
      
      addLocator(LOCATOR_VM_NUM, processManager.getStub(LOCATOR_VM_NUM));
      
      addHost(this);
    }

    @Override
    public VM getVM(int n) {

      if (n == LOCATOR_VM_NUM) {
        return getLocator();
      }
      if (n == DEBUGGING_VM_NUM) {
        //for ease of debugging, pass -1 to get the local VM
        return debuggingVM;
      }

      int oldVMCount = getVMCount();
      if(n >= oldVMCount && n < NUM_VMS) {
        //If we don't have a VM with that number, dynamically create it.
        try {
          for(int i = oldVMCount; i <= n; i++) {
            processManager.launchVM(i);
          }
          processManager.waitForVMs(STARTUP_TIMEOUT);

          for(int i = oldVMCount; i <= n; i++) {
            addVM(i, processManager.getStub(i));
          }

        } catch (IOException | InterruptedException | NotBoundException e) {
          throw new RuntimeException("Could not dynamically launch vm + " + n, e);
        }
      }
      
      return super.getVM(n);
    }
  }
}
