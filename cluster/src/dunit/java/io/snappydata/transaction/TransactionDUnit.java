/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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
package io.snappydata.transaction;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.cache.*;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;
import com.gemstone.gemfire.internal.cache.control.ResourceManagerStats;
import com.gemstone.gemfire.internal.cache.locks.ExclusiveSharedSynchronizer;
import com.gemstone.gnu.trove.TIntHashSet;
import com.pivotal.gemfirexd.Attribute;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.FabricService;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.client.ClientXid;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdConnectionHolder;
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdConnectionWrapper;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedPreparedStatement;
import com.pivotal.gemfirexd.internal.jdbc.EmbeddedXADataSource;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;
import io.snappydata.Lead;
import io.snappydata.ServiceManager;
import io.snappydata.cluster.ClusterManagerTestBase;
import io.snappydata.jdbc.ClientXADataSource;
import io.snappydata.test.dunit.AsyncInvocation;
import io.snappydata.test.dunit.DistributedTestBase;
import io.snappydata.test.dunit.Host;
import io.snappydata.test.dunit.SerializableRunnable;
import io.snappydata.test.dunit.VM;
import io.snappydata.test.dunit.standalone.AnyCyclicBarrier;
import io.snappydata.test.util.TestException;
import org.apache.derbyTesting.junit.JDBC;

@SuppressWarnings("serial")
public class TransactionDUnit extends ClusterManagerTestBase {

  Properties bootProps = new Properties();

  int locatorPort;

  transient protected final List<VM> serverVMs = new ArrayList<VM>();

  @Override
  public void beforeClass(){
    super.beforeClass();
    serverVMs.add(vm0());
    serverVMs.add(vm1());
    serverVMs.add(vm2());
    serverVMs.add(vm3());
  }

  final static String netProtocol = "jdbc:snappydata://";

  private Connection getClientConnection(boolean routeQuery) {
    String driver = "io.snappydata.jdbc.ClientDriver";
    try {
      Class.forName(driver).newInstance();
      String url;
      final InetAddress localHost = SocketCreator.getLocalHost();
      String hostName = localHost.getHostName();
      if (!routeQuery) {
        url = netProtocol + hostName + '[' + locatorPort + "]/";
      } else {
        url = netProtocol + hostName + '[' + locatorPort + "]/";
      }
      return DriverManager.getConnection(url);
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

  private void startSnappyLead(int locatorPort, Properties props) throws SQLException {
    props.setProperty("locators", "localhost[" + locatorPort + ']');
    props.setProperty("jobserver.enabled", "false");
    props.setProperty("isTest", "true");
    Lead server = ServiceManager.getLeadInstance();
    server.start(props);
    assert (server.status() == FabricService.State.RUNNING);
  }

  protected volatile boolean gotConflict = false;

  protected volatile Throwable threadEx;

  protected static volatile boolean failed;

  protected final static String DISKSTORE = "TestPersistenceDiskStore";


  public void serverExecute(int serverNum, Runnable runnable) throws Exception {
    getServerVM(serverNum).invoke(runnable);
  }

  public final VM getServerVM(int serverNum) {
    assertTrue("Server number should be positive and not exceed total "
        + "number of servers: " + this.serverVMs.size(), serverNum > 0
        && serverNum <= this.serverVMs.size());
    return this.serverVMs.get(serverNum - 1);
  }

  public TransactionDUnit(String name) {
    super(name);
  }

  // Uncomment after fixing
  public void DISABLED_testTransactionalInsertAsSubSelects_diffNullable() throws Exception {
    ////startVMs(1, 2);
    Connection conn = getClientConnection(false);
    conn.setTransactionIsolation(getIsolationLevel());
    Statement st = conn.createStatement();
    st.execute("Create table t1 (c1 int not null , c2 int not null, "
        + "primary key(c1)) partition by primary key" + getSuffix());
    st.execute("create table t2 (c1 int, c2 int, primary key(c1)) " +
        "partition by primary key" + getSuffix());
    st.execute("insert into t1 values (1,1)");
    st.execute("insert into t1 values (2,2)");
    st.execute("insert into t2 select * from t1");
    conn.commit();
    conn = getClientConnection(false);
    conn.setTransactionIsolation(getIsolationLevel());
    ResultSet rs = conn.createStatement().executeQuery("select count(*) from t2");
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertFalse(rs.next());
    rs = conn.createStatement().executeQuery("select * from t2");
    assertTrue(rs.next());
    assertTrue((rs.getInt(1) == 1) || (rs.getInt(1) == 2));
    assertTrue((rs.getInt(2) == 1) || (rs.getInt(2) == 2));
    assertTrue(rs.next());
    assertTrue((rs.getInt(1) == 1) || (rs.getInt(1) == 2));
    assertTrue("rs.getInt(2)=" + rs.getInt(2), (rs.getInt(2) == 1) || (rs.getInt(2) == 2));
    assertFalse(rs.next());
  }

  public void testTransactionalInsertAsSubSelects() throws Exception {

    vm0().invoke(getClass(), "startNetworkServer");

    Connection conn = getClientConnection(false);
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    Statement st = conn.createStatement();
    st.execute("Create table t1 (c1 int not null , c2 int not null, "
        + "primary key(c1)) partition by primary key" + getSuffix());
    st.execute("create table t2 (c1 int, c2 int not null, primary key(c1)) " +
        "partition by primary key" + getSuffix());
    st.execute("insert into t1 values (1,1)");
    st.execute("insert into t1 values (2,2)");
    st.execute("insert into t2 select * from t1");
    conn.commit();
    conn = getClientConnection(false);
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    ResultSet rs = conn.createStatement().executeQuery("select count(*) from t2");
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertFalse(rs.next());
    rs = conn.createStatement().executeQuery("select * from t2");
    assertTrue(rs.next());
    assertTrue((rs.getInt(1) == 1) || (rs.getInt(1) == 2));
    assertTrue((rs.getInt(2) == 1) || (rs.getInt(2) == 2));
    assertTrue(rs.next());
    assertTrue((rs.getInt(1) == 1) || (rs.getInt(1) == 2));
    assertTrue((rs.getInt(2) == 1) || (rs.getInt(2) == 2));
    assertFalse(rs.next());
  }

  /**
   * Test transactional inserts on replicated table.
   *
   * @throws Exception
   */
  public void _testTransactionalInsertOnReplicatedTable() throws Exception {
    //startVMs(1, 2);
    Connection conn = getClientConnection(false);
    conn.setAutoCommit(false);
    Statement st = conn.createStatement();
    st.execute("Create table t1 (c1 int not null , c2 int not null, "
        + "primary key(c1)) replicate" + getSuffix());
    conn.commit();
    conn.setTransactionIsolation(getIsolationLevel());
    // check for get without any TXState created yet on servers (#42819)
    ResultSet rs = st.executeQuery("select * from t1 where c1=10");
    assertFalse(rs.next());
    st.execute("insert into t1 values (10, 10)");

    conn.rollback();// rollback.

    rs = st.executeQuery("Select * from t1");
    assertFalse("ResultSet should be empty ", rs.next());
    rs.close();

    st.execute("insert into t1 values (10, 10)");
    st.execute("insert into t1 values (20, 20)");

    conn.commit(); // commit two rows.

    st.execute("insert into t1 values (30, 30)");

    final PreparedStatement pstmt = conn.prepareStatement("Select * from t1");

    final long connId = ((EmbedConnection)conn).getConnectionID();
    final long stmtId = ((EmbedPreparedStatement)pstmt).getID();
    final TXId txId = TXManagerImpl.getCurrentTXId();
    // attach an observer to check for statement close before commit
    final TransactionObserver observer = new TransactionObserverAdapter() {
      private static final long serialVersionUID = 3946313849981555748L;

      @Override
      public void duringIndividualCommit(TXStateProxy tx, Object callbackArg) {
        final GfxdConnectionWrapper wrapper = GfxdConnectionHolder.getHolder()
            .getExistingWrapper(connId);
        if (wrapper != null) {
          if (wrapper.getStatementForTEST(stmtId) != null) {
            failed = true;
            fail("received non-null statement for ID " + stmtId);
          }
        }
      }
    };
    invokeInEveryVM(new SerializableRunnable() {
      @Override
      public void run() {
        failed = false;
        final GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        final TXStateProxy tx;
        if (cache != null
            && (tx = cache.getTxManager().getHostedTXState(txId)) != null) {
          tx.setObserver(observer);
        }
      }
    });

    // check for get without any TXState created yet on servers (#42819)
    rs = st.executeQuery("select * from t1 where c1=20");
    assertTrue(rs.next());
    assertEquals(20, rs.getInt(1));
    assertEquals(20, rs.getInt(2));
    assertFalse(rs.next());

    rs = pstmt.executeQuery();
    int numRows = 0;
    while (rs.next()) {
      // Checking number of rows returned, since ordering of results
      // is not guaranteed. We can write an order by query for this (another
      // test).
      numRows++;
    }
    assertEquals("ResultSet should contain three rows ", 3, numRows);

    // Close connection, resultset etc...
    rs.close();
    st.close();
    try {
      pstmt.close();
      conn.commit();
    } finally {
      invokeInEveryVM(new SerializableRunnable() {
        @Override
        public void run() {
          final GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
          final TXStateProxy tx;
          if (cache != null
              && (tx = cache.getTxManager().getHostedTXState(txId)) != null) {
            tx.setObserver(null);
          }
          final boolean failure = failed;
          failed = false;
          assertFalse("unexpected failure for statement close assert", failure);
        }
      });
    }

    conn.close();
  }

  /**
   * Test transactional inserts on Partitioned tables.
   *
   * @throws Exception
   */
  public void _testTransactionalInsertOnPartitionedTable() throws Exception {
    //startVMs(1, 1);
    Connection conn = getClientConnection(false);
    conn.setAutoCommit(false);
    Statement st = conn.createStatement();
    st.execute("create schema tran");
    st.execute("Create table tran.t1 (c1 int not null , c2 int not null, "
        + "primary key(c1))" + getSuffix());
    conn.commit();
    conn.setTransactionIsolation(getIsolationLevel());
    st.execute("insert into tran.t1 values (10, 10)");
    st.execute("insert into tran.t1 values (20, 20)");

    conn.rollback();// rollback.

    ResultSet rs = st.executeQuery("Select * from tran.t1");
    assertFalse("ResultSet should be empty ", rs.next());
    rs.close();

    st.execute("insert into tran.t1 values (10, 10)");
    st.execute("insert into tran.t1 values (20, 20)");

    conn.commit(); // commit two rows.
    rs = st.executeQuery("Select * from tran.t1");
    int numRows = 0;
    while (rs.next()) {
      // Checking number of rows returned, since ordering of results
      // is not guaranteed. We can write an order by query for this (another
      // test).
      numRows++;
    }
    assertEquals("ResultSet should contain two rows ", 2, numRows);
    VM vm = vm0();
    vm.invoke(getClass(), "checkData", new Object[]{"TRAN.T1",
        Long.valueOf(2)});

    // Close connection, resultset etc...
    rs.close();
    st.close();
    conn.commit();
    conn.close();
  }

  /**
   * Check local data size.
   *
   * @param regionName region name.
   * @param numEntries number of entries expected.
   */
  public static void checkData(String regionName, long numEntries) {
    final PartitionedRegion r = (PartitionedRegion)Misc
        .getRegionForTable(regionName, true);
    final long localSize = r.getLocalSize();
    assertEquals(
        "Unexpected number of rows in the table " + r.getUserAttribute(),
        numEntries, localSize);
  }

  /**
   * Test conflicts.
   */
  public void _testCommitWithConflicts() throws Exception {
    //startVMs(1, 1);
    Connection conn = getClientConnection(false);
    conn.setAutoCommit(false);
    Statement st = conn.createStatement();
    st.execute("create schema tran");
    st.execute("Create table tran.t1 (c1 int not null , c2 int not null, "
        + "primary key(c1))" + getSuffix());
    conn.commit();
    this.gotConflict = false;
    conn.setTransactionIsolation(getIsolationLevel());
    st.execute("insert into tran.t1 values (10, 10)");
    st.execute("insert into tran.t1 values (20, 10)");
    st.execute("insert into tran.t1 values (30, 10)");
    Region<?, ?> r = Misc.getRegionForTable("TRAN.T1", false);
    assertNotNull(r);
    final boolean[] otherTxOk = new boolean[]{false};
    Thread otherTx = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Connection otherConn = getClientConnection(false);
          otherConn
              .setTransactionIsolation(getIsolationLevel());
          otherConn.setAutoCommit(false);
          Statement otherSt = otherConn.createStatement();
          try {
            otherSt.execute("insert into tran.t1 values (10, 20)");
            otherTxOk[0] = true;
          } catch (SQLException sqle) {
            if ("X0Z02".equals(sqle.getSQLState())) {
              Throwable t = sqle.getCause();
              while (t.getCause() != null) {
                t = t.getCause();
              }
              assertTrue(t instanceof ConflictException);
              gotConflict = true;
              otherConn.rollback();
            } else {
              throw sqle;
            }
          }
        } catch (SQLException sqle) {
          gotConflict = false;
          fail("unexpected exception", sqle);
        }
      }
    });
    otherTx.start();
    otherTx.join();
    assertFalse(otherTxOk[0]);

    assertTrue("expected conflict", this.gotConflict);
    this.gotConflict = false;

    PreparedStatement ps = conn.prepareStatement("select * from tran.t1");

    conn.commit();

    // check that the value should be that of first transaction
    ResultSet rs = ps.executeQuery();
    int[] expectedKeys = new int[]{10, 20, 30};
    int numRows = 0;
    while (rs.next()) {
      ++numRows;
      final int key = rs.getInt(1);
      final int index = Arrays.binarySearch(expectedKeys, key);
      assertTrue("Expected to find the key: " + key, index >= 0);
      expectedKeys[index] = Integer.MIN_VALUE + 1;
      Arrays.sort(expectedKeys);
      assertEquals("Second column should be 10", 10, rs.getInt(2));
    }
    assertEquals("ResultSet should have three rows", 3, numRows);

    // Remove the key - value pair.
    r.destroy(TestUtil.getGemFireKey(10, r));

    Statement st2 = conn.createStatement();
    st2.execute("insert into tran.t1 values (10, 30)");
    conn.commit();
    expectedKeys = new int[]{10, 20, 30};
    rs = ps.executeQuery();
    numRows = 0;
    while (rs.next()) {
      ++numRows;
      final int key = rs.getInt(1);
      final int index = Arrays.binarySearch(expectedKeys, key);
      assertTrue("Expected to find the key: " + key, index >= 0);
      expectedKeys[index] = Integer.MIN_VALUE + 1;
      Arrays.sort(expectedKeys);
      if (key == 10) {
        assertEquals("Second column should be 30 for key 10", 30, rs.getInt(2));
      } else {
        assertEquals("Second column should be 10", 10, rs.getInt(2));
      }
    }
    assertEquals("ResultSet should have three rows", 3, numRows);

    rs.close();
    st2.close();
    conn.commit();
    conn.close();
  }

  /**
   * Test transactional insert on Partitioned and Replicated Tables (colocated).
   *
   * @throws Exception on failure.
   */
  public void _testCommitOnPartitionedAndReplicatedTables() throws Exception {
    //startVMs(1, 1);
    Connection conn = getClientConnection(false);
    Statement st = conn.createStatement();
    st.execute("Create table t1 (c1 int not null , c2 int not null, "
        + "primary key(c1)) replicate" + getSuffix());
    // partitioned table.
    st.execute("Create table t2 (c1 int not null , c2 int not null, "
        + "primary key(c1)) " + getSuffix());
    conn.commit();
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    // operations on partitioned followed by replicated.
    st.execute("insert into t2 values(10,10)");
    st.execute("insert into t1 values (10, 10)");
    st.execute("insert into t1 values (20, 20)");

    conn.commit(); // commit three rows.
    ResultSet rs = st.executeQuery("Select * from t1");
    int numRows = 0;
    while (rs.next()) {
      // Checking number of rows returned, since ordering of results
      // is not guaranteed.
      numRows++;
    }
    assertEquals("ResultSet should contain two rows ", 2, numRows);
    numRows = 0;
    rs = st.executeQuery("Select * from t2");
    while (rs.next()) {
      numRows++;
    }
    assertEquals("ResultSet should have one row", 1, numRows);
    // Close connection, resultset etc...
    rs.close();
    st.close();

    checkConnCloseExceptionForReadsOnly(conn);

    conn.commit();
    conn.close();
  }

  /**
   * Test transactional selects.
   *
   * @throws Exception
   */
  public void _testSelectIsolated() throws Exception {
    //startVMs(1, 2);
    Connection conn = getClientConnection(false);
    Statement st = conn.createStatement();
    st.execute("Create table t1 (c1 int not null , c2 int not null, "
        + "primary key(c1)) replicate" + getSuffix());
    // partitioned table.
    st.execute("Create table t2 (c1 int not null , c2 int not null, "
        + "primary key(c1)) " + getSuffix());
    conn.commit();
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);

    // operations on partitioned followed by replicated.
    st.execute("insert into t2 values(10,10)");
    st.execute("insert into t1 values (10, 10)");
    st.execute("insert into t1 values (20, 20)");

    // conn.commit(); // commit three rows.
    ResultSet rs = st.executeQuery("Select * from t1");
    int numRows = 0;
    while (rs.next()) {
      // Checking number of rows returned, since ordering of results
      // is not guaranteed. We can write an order by query for this (another
      // test).
      numRows++;
    }
    assertEquals("ResultSet should contain two rows ", 2, numRows);
    rs.close();
    conn.commit();

    rs = st.executeQuery("Select * from t1");
    numRows = 0;
    while (rs.next()) {
      // Checking number of rows returned, since ordering of results
      // is not guaranteed. We can write an order by query for this (another
      // test).
      numRows++;
    }
    assertEquals("ResultSet should contain two rows ", 2, numRows);
    rs.close();
    st.close();
    conn.commit();
    conn.close();
  }

  /**
   * Test insert on colocated partitioned regions.
   *
   * @throws Exception
   */
  public void _testColocatedPrTransaction() throws Exception {
    //startVMs(1, 2);
    // TestUtil.loadDriver();

    Connection conn = getClientConnection(false);
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    Statement st = conn.createStatement();
    st.execute("create table T1 ( PkCol1 int not null, PkCol2 int not null , "
        + "col3 int, col4 int, col5 varchar(10), Primary Key (PkCol1, PkCol2) ) "
        + "Partition by column (PkCol1)" + getSuffix());

    st.execute("create table T2 (PkCol1 int not null, PkCol2 int not null, "
        + " col3 int, col4 varchar(10)) Partition by column (PkCol1) "
        + " colocate with (T1)" + getSuffix());
    conn.commit();
    st.execute("insert into t1 values(10, 10, 10, 10, 'XXXX1')");
    st.execute("insert into t2 values(10, 10, 10, 'XXXX1')");
    conn.commit();

    ResultSet rs = st.executeQuery("select * from t1");
    int numRows = 0;
    while (rs.next()) {
      numRows++;
    }
    assertEquals("Select should return one row", 1, numRows);
    rs = st.executeQuery("select * from t2");
    numRows = 0;
    while (rs.next()) {
      numRows++;
    }
    assertEquals("Select should return one row", 1, numRows);

    rs.close();

    st.execute("insert into t1 values(20, 20, 20, 20, 'XXXX2')");
    st.execute("insert into t2 values(20, 20, 20, 'XXXX2')");
    conn.rollback();
    rs = st.executeQuery("select * from t1");
    numRows = 0;
    while (rs.next()) {
      numRows++;
    }
    assertEquals("Select should return one row", 1, numRows);
    rs = st.executeQuery("select * from t2");
    numRows = 0;
    while (rs.next()) {
      numRows++;
    }
    assertEquals("Select should return one row", 1, numRows);
    rs.close();

    st.execute("insert into t1 values(20, 20, 20, 20, 'XXXX2')");
    st.execute("insert into t2 values(20, 20, 20, 'XXXX2')");
    conn.commit();
    rs = st.executeQuery("select * from t1");
    numRows = 0;
    while (rs.next()) {
      numRows++;
    }
    assertEquals("Select should return two rows", 2, numRows);
    rs = st.executeQuery("select * from t2");
    numRows = 0;
    while (rs.next()) {
      numRows++;
    }
    assertEquals("Select should return two rows", 2, numRows);

    st.close();
    conn.commit();
    conn.close();

  }

  /**
   * Test multiple commit and rollback.
   *
   * @throws Exception
   */
  public void _testCommitAndRollBack() throws Exception {
    //startVMs(1, 2);
    // TestUtil.loadDriver();

    Properties props = new Properties();
    props.setProperty(Attribute.TX_SYNC_COMMITS, "true");
    final Connection conn = TestUtil.getConnection(props);
    conn.setAutoCommit(false);
    Statement st = conn.createStatement();
    st.execute("create table T1 ( PkCol1 int not null, PkCol2 int not null , "
        + "col3 int, col4 int, col5 varchar(10), Primary Key (PkCol1, PkCol2)) "
        + "Partition by column (PkCol1)" + getSuffix());
    conn.commit();

    conn.setTransactionIsolation(getIsolationLevel());
    PreparedStatement psInsert = conn.prepareStatement("insert into t1 "
        + "values(?, ?, ?, ?, ?)");
    for (int i = 0; i < 1000; i++) {
      psInsert.setInt(1, i);
      psInsert.setInt(2, i);
      psInsert.setInt(3, i);
      psInsert.setInt(4, i);
      psInsert.setString(5, "XXXX" + i);
      psInsert.executeUpdate();
      if ((i % 2) == 0) {
        conn.commit();
      } else {
        conn.rollback();
      }
    }

    TXManagerImpl.waitForPendingCommitForTest();
    // approx. 240 commits/rollbacks gets distributed across 2 nodes. adjust these numbers to a little lower value
    // if unbalanced commits/rollbacks happen.
    checkTxStatistics("commit-afterInserts", 240, 240, 240, 240, 500, 501);

    ResultSet rs = st.executeQuery("select * from t1");
    int numRows = 0;
    while (rs.next()) {
      numRows++;
    }

    assertEquals("Table should have 500 rows", 500, numRows);
    rs.close();
    st.close();
    conn.commit();
    conn.close();
    TXManagerImpl.waitForPendingCommitForTest();
    checkTxStatistics("commit-afterSelects", 240, 240, 240, 240, 500, 501);
  }

  private void checkTxStatistics(final String comment, final int rCommits1,
      final int rRollback1, final int rCommits2, final int rRollback2,
      final int lCommit, final int lRollback) throws Exception {

    final int[] isRemoteCheck = new int[]{-1};

    final SerializableRunnable checkStats = new SerializableRunnable(
        "check transaction counts") {
      @Override
      public void run() throws CacheException {
        final InternalDistributedSystem dsys = Misc.getDistributedSystem();
        final StatisticsType stType = dsys.findType("CachePerfStats");
        Statistics[] stats = dsys.findStatisticsByType(stType);
        Statistics cpStats = null;
        for (Statistics s : stats) {
          if ("cachePerfStats".equals(s.getTextId())) {
            getLogWriter().info("SB: " + comment + " picked up stats " + s);
            cpStats = s;
            break;
          }
        }

        int txCommit, txCommitChanges, txFailures, txFailureChanges, txRollbacks, txRollbackChanges;
        long txCommitTime, txSuccessLifeTime, txFailureTime, txFailedLifeTime, txRollbackTime, txRollbackLifeTime;

        if (isRemoteCheck[0] == -1) { // local
          txCommit = cpStats.getInt("txCommits");
          txCommitChanges = cpStats.getInt("txCommitChanges");
          txCommitTime = cpStats.getLong("txCommitTime");
          txSuccessLifeTime = cpStats.getLong("txSuccessLifeTime");

          txFailures = cpStats.getInt("txFailures");
          txFailureChanges = cpStats.getInt("txFailureChanges");
          txFailureTime = cpStats.getLong("txFailureTime");
          txFailedLifeTime = cpStats.getLong("txFailedLifeTime");

          txRollbacks = cpStats.getInt("txRollbacks");
          txRollbackChanges = cpStats.getInt("txRollbackChanges");
          txRollbackTime = cpStats.getLong("txRollbackTime");
          txRollbackLifeTime = cpStats.getLong("txRollbackLifeTime");

        } else {

          txCommit = cpStats.getInt("txRemoteCommits");
          txCommitChanges = cpStats.getInt("txRemoteCommitChanges");
          txCommitTime = cpStats.getLong("txRemoteCommitTime");
          txSuccessLifeTime = cpStats.getLong("txRemoteSuccessLifeTime");

          txFailures = cpStats.getInt("txRemoteFailures");
          txFailureChanges = cpStats.getInt("txRemoteFailureChanges");
          txFailureTime = cpStats.getLong("txRemoteFailureTime");
          txFailedLifeTime = cpStats.getLong("txRemoteFailedLifeTime");

          txRollbacks = cpStats.getInt("txRemoteRollbacks");
          txRollbackChanges = cpStats.getInt("txRemoteRollbackChanges");
          txRollbackTime = cpStats.getLong("txRemoteRollbackTime");
          txRollbackLifeTime = cpStats.getLong("txRemoteRollbackLifeTime");
        }

        getLogWriter().info("server " + isRemoteCheck[0] + " isRemote=" + (isRemoteCheck[0] != -1));
        getLogWriter().info("txCommits=" + txCommit + " txCommitChanges=" + txCommitChanges + " txCommitTime=" + txCommitTime + " txSuccessLifeTime=" + txSuccessLifeTime);
        getLogWriter().info("txFailures=" + txFailures + " txFailureChanges=" + txFailureChanges + " txFailureTime=" + txFailureTime + " txFailedLifeTime=" + txFailedLifeTime);
        getLogWriter().info("txRollbacks=" + txRollbacks + " txRollbackChanges=" + txRollbackChanges + " txRollbackTime=" + txRollbackTime + " txRollbackLifeTime=" + txRollbackLifeTime);

        if (isRemoteCheck[0] == -1) { // local
          assertTrue("txCommit=" + txCommit + "," + "lCommits=" + lCommit, txCommit >= lCommit);
        } else if (isRemoteCheck[0] == 1) {
          assertTrue("server " + isRemoteCheck[0] + "txCommit=" + txCommit + "," + "rCommits=" + rCommits1, txCommit >= rCommits1);
        } else if (isRemoteCheck[0] == 2) {
          assertTrue("server " + isRemoteCheck[0] + " txCommit=" + txCommit + "," + "rCommits=" + rCommits2, txCommit >= rCommits2);
        }

      }
    };
    checkStats.run();
    isRemoteCheck[0] = 1;
    serverExecute(1, checkStats);
    isRemoteCheck[0] = 2;
    serverExecute(2, checkStats);

  }

  /**
   * Test non transactional commit and rollback.
   *
   * @throws Exception
   */
  public void _testNonTransactionalCommitAndRollback() throws Exception {
    //startVMs(1, 2);
    // TestUtil.loadDriver();

    Connection conn = getClientConnection(false);
    conn.setTransactionIsolation(Connection.TRANSACTION_NONE);
    conn.setAutoCommit(false);
    Statement st = conn.createStatement();
    st.execute("create table T1 ( PkCol1 int not null, PkCol2 int not null , "
        + "col3 int, col4 int, col5 varchar(10), Primary Key (PkCol1, PkCol2) ) "
        + "Partition by column (PkCol1)" + getSuffix());
    conn.commit();

    PreparedStatement psInsert = conn.prepareStatement("insert into t1 "
        + "values(?, ?, ?, ?, ?)");
    int maxRows = 5000;
    for (int i = 0; i < maxRows; i++) {
      psInsert.setInt(1, i);
      psInsert.setInt(2, i);
      psInsert.setInt(3, i);
      psInsert.setInt(4, i);
      psInsert.setString(5, "XXXX" + i);
      psInsert.executeUpdate();
      if ((i % 2) == 0) {
        conn.commit();
      } else {
        conn.rollback();
      }
    }
    ResultSet rs = st.executeQuery("select * from t1");
    int numRows = 0;
    while (rs.next()) {
      numRows++;
    }
    assertEquals("Table should have 5000 rows", maxRows, numRows);
    rs.close();
    st.close();
    conn.commit();
    conn.close();
  }

  public void _testNonColocatedInsertByPartitioning() throws Exception {
    startServerVMs(1, 0, "sg1");
    startServerVMs(1, 0, "sg2");
    startClientVMs(1, 0, null);
    // TestUtil.loadDriver();

    Connection conn = getClientConnection(false);
    System.out.println("XXXX the type of conneciton :  " + conn);
    Statement st = conn.createStatement();
    st.execute("create schema test default server groups (sg1, sg2)");
    st.execute("create table test.t1 ( PkCol1 int not null, PkCol2 int not null , "
        + "col3 int, col4 int, col5 varchar(10), Primary Key (PkCol1, PkCol2) ) "
        + "Partition by column (PkCol1) server groups (sg1)" + getSuffix());

    st.execute("create table test.t2 (PkCol1 int not null, PkCol2 int not null, "
        + " col3 int, col4 varchar(10)) Partition by column (PkCol1)"
        + " server groups (sg2)" + getSuffix());
    conn.commit();
    // conn.setTransactionIsolation(getIsolationLevel());
    st.execute("insert into test.t1 values(10, 10, 10, 10, 'XXXX1')");
    st.execute("insert into test.t2 values(10, 10, 10, 'XXXX1')");
    conn.commit();

  }

  /**
   * Test transactional key based updates.
   *
   * @throws Exception
   */
  public void _testTransactionalKeyBasedUpdates() throws Exception {
    startServerVMs(2, 0, "sg1");
    startClientVMs(1, 0, null);
    Connection conn = getClientConnection(false);
    conn.setAutoCommit(false);
    System.out.println("XXXX the type of conneciton :  " + conn);
    Statement st = conn.createStatement();
    st.execute("create schema test default server groups (sg1, sg2)");
    st.execute("create table test.t1 ( PkCol1 int not null, PkCol2 int not null , "
        + "col3 int, col4 int, col5 varchar(10), Primary Key (PkCol1) ) "
        + "Partition by column (PkCol1) server groups (sg1)" + getSuffix());
    conn.commit();
    conn.setTransactionIsolation(getIsolationLevel());
    PreparedStatement psInsert = conn.prepareStatement("insert into test.t1 "
        + "values(?, 10, 10, 10, 'XXXX1')");
    // st.execute("insert into test.t1 values(10, 10, 10, 10, 'XXXX1')");
    for (int i = 0; i < 1000; i++) {
      psInsert.setInt(1, i);
      psInsert.executeUpdate();
      conn.commit();
    }
    // conn.commit();
    ResultSet rs = st.executeQuery("select * from test.t1");
    int numRows = 0;
    while (rs.next()) {
      assertEquals("Column value should be 10", 10, rs.getInt(3));
      assertEquals("Column value should be 10", 10, rs.getInt(4));
      assertEquals("Column value should be XXXX1", "XXXX1", rs.getString(5)
          .trim());
      numRows++;
    }
    assertEquals("Numbers of rows in resultset should be one", 1000, numRows);
    // conn.commit();
    PreparedStatement psUpdate = conn.prepareStatement("update test.t1 set "
        + "col3 = 20, col4 = 20, col5 = 'changed' where PkCol1=?");
    // st.execute("update test.t1 set col3 = 20, col4 = 20, col5 = 'changed' where PkCol1=10");
    for (int i = 0; i < 1000; i++) {
      psUpdate.setInt(1, i);
      psUpdate.executeUpdate();
      conn.commit();
    }
    // conn.commit();
    rs = st.executeQuery("select * from test.t1");
    numRows = 0;
    while (rs.next()) {
      assertEquals("Column value should change", 20, rs.getInt(3));
      assertEquals("Columns value should change", 20, rs.getInt(4));
      assertEquals("Columns value should change", "changed", rs.getString(5)
          .trim());
      numRows++;
    }
    assertEquals("Numbers of rows in resultset should be one", 1000, numRows);
    rs.close();
    st.close();
    conn.commit();
    conn.close();

  }

  /**
   * Test updates on tables partitioned by PK.
   *
   * @throws Exception
   */
  public void _testTransactionalKeyBasedUpdatePartitionedByPk() throws Exception {
    startServerVMs(2, 0, "sg1");
    startClientVMs(1, 0, null);
    Connection conn = getClientConnection(false);
    conn.setAutoCommit(false);
    Statement st = conn.createStatement();
    st.execute("create schema test default server groups (sg1, sg2)");
    st.execute("create table test.t1 ( PkCol1 int not null, PkCol2 int not null , "
        + "col3 int, col4 int, col5 varchar(10), Primary Key (PkCol1) ) "
        + "Partition by Primary Key server groups (sg1) redundancy 1" + getSuffix());

    conn.setTransactionIsolation(getIsolationLevel());
    PreparedStatement psInsert = conn.prepareStatement("insert into test.t1 "
        + "values(?, 10, 10, 10, 'XXXX1')");
    // st.execute("insert into test.t1 values(10, 10, 10, 10, 'XXXX1')");
    for (int i = 0; i < 1000; i++) {
      psInsert.setInt(1, i);
      psInsert.executeUpdate();
      conn.commit();
    }
    ResultSet rs = st.executeQuery("select * from test.t1");
    int numRows = 0;
    while (rs.next()) {
      assertEquals("Column value should be 10", 10, rs.getInt(3));
      assertEquals("Column value should be 10", 10, rs.getInt(4));
      assertEquals("Column value should be XXXX1", "XXXX1", rs.getString(5)
          .trim());
      numRows++;
    }
    assertEquals("Numbers of rows in resultset should be one", 1000, numRows);

    PreparedStatement psUpdate = conn.prepareStatement("update test.t1 set "
        + "col3 = 20, col4 = 20, col5 = 'changed' where PkCol1=?");
    // st.execute("update test.t1 set col3 = 20, col4 = 20, col5 = 'changed' where PkCol1=10");
    for (int i = 0; i < 1000; i++) {
      psUpdate.setInt(1, i);
      psUpdate.executeUpdate();
      conn.commit();
    }

    rs = st.executeQuery("select * from test.t1");
    numRows = 0;
    while (rs.next()) {
      assertEquals("Column value should change", 20, rs.getInt(3));
      assertEquals("Columns value should change", 20, rs.getInt(4));
      assertEquals("Columns value should change", "changed", rs.getString(5)
          .trim());
      numRows++;
    }
    assertEquals("Numbers of rows in resultset should be one", 1000, numRows);
    rs.close();
    st.close();
    conn.commit();
    conn.close();

  }

  /**
   * DDL followed imediately by commit and then DML.
   *
   * @throws Exception
   */
  public void _testNetworkTransactionDDLFollowedByCommitThenDML()
      throws Exception {

    // start three network servers
    startServerVMs(1, 0, null);
    final int netPort = startNetworkServer(1, null, null);
    final Connection conn = TestUtil.getNetConnection(netPort,
        "/;user=q;password=q", null);
    conn.setAutoCommit(false);
    conn.commit();
    conn.setTransactionIsolation(getIsolationLevel());

    Statement st = conn.createStatement();
    st.execute("create table t1 (PkCol1 int not null, Col2 int not null,"
        + " Primary Key (PkCol1))" + getSuffix());
    // Committing the ddl.
    conn.commit();
    PreparedStatement ps = conn.prepareStatement("insert into t1 values (?,?)");
    for (int i = 0; i < 1; i++) {
      ps.setInt(1, i);
      ps.setInt(2, i);
      ps.executeUpdate();
    }
    ResultSet rs = st.executeQuery("select * from t1");
    assertTrue(rs.next());
    conn.commit();
    rs = st.executeQuery("select * from t1");
    assertTrue(rs.next());
    conn.commit();
    rs.close();
    st.close();
    conn.close();

  }

  /**
   * DDL followed by a DML with not commit in between.
   *
   * @throws Exception
   */
  public void _testNetworkDDLFollowedByInsert() throws Exception {
    // start three network servers
    startServerVMs(1, 0, null);
    final int netPort = startNetworkServer(1, null, null);
    final Connection conn = TestUtil.getNetConnection(netPort,
        "/;user=q;password=q", null);
    conn.setAutoCommit(false);
    conn.commit();
    conn.setTransactionIsolation(getIsolationLevel());

    Statement st = conn.createStatement();
    st.execute("create table t1 (PkCol1 int not null, Col2 int not null,"
        + " Primary Key (PkCol1))" + getSuffix());

    final int numRows = 100;
    PreparedStatement ps = conn.prepareStatement("insert into t1 values (?,?)");
    for (int i = 0; i < numRows; i++) {
      ps.setInt(1, i);
      ps.setInt(2, i);
      ps.executeUpdate();
    }
    ResultSet rs = st.executeQuery("select * from t1");
    for (int i = 0; i < numRows; i++) {
      assertTrue(rs.next());
    }
    conn.commit();
    rs = st.executeQuery("select * from t1");
    for (int i = 0; i < numRows; i++) {
      assertTrue(rs.next());
    }
    conn.commit();

    // also check for READ_UNCOMMITTED
    conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
    assertEquals(Connection.TRANSACTION_READ_COMMITTED,
        conn.getTransactionIsolation());
    for (int i = numRows; i < 2 * numRows; i++) {
      ps.setInt(1, i);
      ps.setInt(2, i);
      ps.executeUpdate();
    }
    rs = st.executeQuery("select * from t1");
    for (int i = numRows; i < 2 * numRows; i++) {
      assertTrue(rs.next());
    }
    conn.commit();
    rs = st.executeQuery("select * from t1");
    for (int i = numRows; i < 2 * numRows; i++) {
      assertTrue(rs.next());
    }
    conn.commit();

    rs.close();
    st.close();
    conn.close();
  }

  public void _testTransactionalUpdates() throws Exception {
    startServerVMs(1, 0, null);
    final int netPort = startNetworkServer(1, null, null);
    final Connection conn = TestUtil.getNetConnection(netPort,
        "/;user=q;password=q", null);
    conn.commit();
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    Statement st = conn.createStatement();
    st.execute("create table t1 (PkCol1 int not null, Col2 varchar(10) not null,"
        + " Primary Key (PkCol1))" + getSuffix());

    final int numRows = 200;
    PreparedStatement ps = conn.prepareStatement("insert into t1 values (?,?)");
    PreparedStatement ps1 = conn.prepareStatement("select * from t1");
    for (int i = 0; i < numRows; i++) {
      ps.setInt(1, i);
      ps.setString(2, "XXXX" + i);
      ps.executeUpdate();
    }
    // query should see TX changes
    ResultSet rs = ps1.executeQuery();
    for (int i = 0; i < numRows; i++) {
      assertTrue(rs.next());
      assertTrue(rs.getString(2).startsWith("XXXX"));
    }
    assertFalse(rs.next());
    // rollback should see no changes
    conn.rollback();
    conn.commit();
    rs = ps1.executeQuery();
    assertFalse(rs.next());
    // commit with changes again
    for (int i = 0; i < numRows; i++) {
      ps.setInt(1, i);
      ps.setString(2, "XXXX" + i);
      ps.executeUpdate();
    }
    conn.commit();
    // check results
    rs = ps1.executeQuery();
    for (int i = 0; i < numRows; i++) {
      assertTrue(rs.next());
      assertTrue(rs.getString(2).startsWith("XXXX"));
    }
    assertFalse(rs.next());
    // check updates
    PreparedStatement select = conn
        .prepareStatement("select * from t1 where t1.pkCol1 = ?");
    PreparedStatement psUpdate = conn
        .prepareStatement("update t1 set col2 = ? where PkCol1 = ?");
    for (int i = 0; i < numRows; i++) {
      select.setInt(1, i);
      rs = select.executeQuery();
      assertTrue("unexpected no result for i=" + i, rs.next());
      assertTrue(rs.getString("Col2").trim().equalsIgnoreCase("XXXX" + i));
      psUpdate.setString(1, "Updated");
      psUpdate.setInt(2, i);
      psUpdate.executeUpdate();
      rs = select.executeQuery();
      assertTrue(rs.next());
      assertTrue(rs.getString("Col2").trim().equalsIgnoreCase("Updated"));
      conn.commit();
      rs = select.executeQuery();
      assertTrue(rs.next());
      assertTrue(rs.getString("Col2").trim().equalsIgnoreCase("Updated"));
    }
    conn.commit();
    rs.close();
    st.close();
    conn.close();
  }

  public void _testIndexMaintenanceOnPrimaryAndSecondary() throws Exception {
    startServerVMs(2, 0, "sg1");
    startClientVMs(1, 0, null);
    Properties props = new Properties();
    props.setProperty(Attribute.TX_SYNC_COMMITS, "true");
    final Connection conn = TestUtil.getConnection(props);
    Statement st = conn.createStatement();
    st.execute("create schema test default server groups (sg1, sg2)");
    st.execute("create table test.t1 ( PkCol1 int not null, PkCol2 int "
        + "not null , col3 int, col4 int, col5 varchar(10), Primary Key(PkCol1)"
        + ") Partition by Primary Key server groups (sg1) redundancy 1" + getSuffix());
    conn.commit();
    st.execute("create index IndexCol4 on test.t1 (col4)");
    conn.commit();

    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    final int numRows = 10;
    VM server1 = this.serverVMs.get(0);
    VM server2 = this.serverVMs.get(1);
    server1.invoke(getClass(), "installIndexObserver",
        new Object[]{"test.IndexCol4", null});
    server2.invoke(getClass(), "installIndexObserver",
        new Object[]{"test.IndexCol4", null});
    PreparedStatement psInsert = conn.prepareStatement("insert into test.t1 "
        + "values(?, 10, 10, 10, 'XXXX1')");
    for (int i = 0; i < numRows; i++) {
      psInsert.setInt(1, i);
      psInsert.executeUpdate();
      conn.commit();
    }

    server1.invoke(getClass(), "checkIndexAndReset",
        new Object[]{Integer.valueOf(numRows), Integer.valueOf(0)});
    server2.invoke(getClass(), "checkIndexAndReset",
        new Object[]{Integer.valueOf(numRows), Integer.valueOf(0)});

    PreparedStatement psUpdate = conn.prepareStatement("update test.t1 set "
        + "col3 = 20, col4 = 20, col5 = 'changed' where PkCol1=?");
    for (int i = 0; i < numRows; i++) {
      psUpdate.setInt(1, i);
      psUpdate.executeUpdate();
      conn.commit();
    }

    server1.invoke(getClass(), "checkIndexAndReset", new Object[]{
        Integer.valueOf(numRows * 2), Integer.valueOf(numRows)});
    server2.invoke(getClass(), "checkIndexAndReset", new Object[]{
        Integer.valueOf(numRows * 2), Integer.valueOf(numRows)});

    server1.invoke(getClass(), "resetIndexObserver");
    server2.invoke(getClass(), "resetIndexObserver");

    st.close();
    conn.close();
  }

  public void _testXATransactionFromClient_commit() throws Exception {
    startServerVMs(2, 0, null);
    startClientVMs(1, 0, null);
    final int netport = startNetworkServer(1, null, null);
    serverSQLExecute(1, "create schema test");
    serverSQLExecute(1, "create table test.XATT2 (intcol int not null, text varchar(100) not null)" + getSuffix());
    serverSQLExecute(1, "insert into test.XATT2 values (1, 'ONE')");

    ClientXADataSource xaDataSource = (ClientXADataSource)TestUtil
        .getXADataSource(TestUtil.NetClientXADsClassName);
    byte[] gid = new byte[64];
    byte[] bid = new byte[64];
    for (int i = 0; i < 64; i++) {
      gid[i] = (byte)i;
      bid[i] = (byte)(64 - i);
    }
    Xid xid = new ClientXid(0x1234, gid, bid);

    String localhost = SocketCreator.getLocalHost().getHostName();
    xaDataSource.setServerName(localhost);
    xaDataSource.setPortNumber(netport);
    xaDataSource.setDatabaseName("gemfirexd");
    // get the stuff required to execute the global transaction
    XAConnection xaConn = xaDataSource.getXAConnection();
    XAResource xaRes = xaConn.getXAResource();
    Connection conn = xaConn.getConnection();

    // start the transaction with that xid
    xaRes.start(xid, XAResource.TMNOFLAGS);
    conn.setTransactionIsolation(getIsolationLevel());
    // do some work
    Statement stm = conn.createStatement();
    stm.execute("insert into test.XATT2 values (2, 'TWO')");

    String jdbcSQL = "select * from test.XATT2";
    String xmlFile = TestUtil.getResourcesDir() + "/lib/checkQuery.xml";

    sqlExecuteVerify(null, new int[]{1}, jdbcSQL, xmlFile, "before_xa_commit");

    stm.execute("select * from test.XATT2");
    ResultSet r = stm.getResultSet();
    int cnt = 0;
    while (r.next()) {
      cnt++;
    }
    assertEquals(2, cnt);
    xaRes.end(xid, XAResource.TMSUCCESS);
    xaRes.prepare(xid);
    xaRes.commit(xid, false);

    //TXManagerImpl.waitForPendingCommitForTest();

    VM servervm = getServerVM(1);
    servervm.invoke(TransactionDUnit.class, "waitForPendingCommit");
    sqlExecuteVerify(null, new int[]{1}, jdbcSQL, xmlFile, "after_xa_commit");
  }

  public void _testXATransactionFromPeerClient_commit() throws Exception {
    startServerVMs(2, 0, null);
    startClientVMs(1, 0, null);

    serverSQLExecute(1, "create schema test");
    serverSQLExecute(1, "create table test.XATT2 (intcol int not null, text varchar(100) not null)" + getSuffix());
    serverSQLExecute(1, "insert into test.XATT2 values (1, 'ONE')");

    EmbeddedXADataSource xaDataSource = (EmbeddedXADataSource)TestUtil
        .getXADataSource(TestUtil.EmbeddedeXADsClassName);
    byte[] gid = new byte[64];
    byte[] bid = new byte[64];
    for (int i = 0; i < 64; i++) {
      gid[i] = (byte)i;
      bid[i] = (byte)(64 - i);
    }
    Xid xid = new ClientXid(0x1234, gid, bid);

    // get the stuff required to execute the global transaction
    XAConnection xaConn = xaDataSource.getXAConnection();
    XAResource xaRes = xaConn.getXAResource();
    Connection conn = xaConn.getConnection();

    // start the transaction with that xid
    xaRes.start(xid, XAResource.TMNOFLAGS);

    conn.setTransactionIsolation(getIsolationLevel());
    // do some work
    Statement stm = conn.createStatement();
    stm.execute("insert into test.XATT2 values (2, 'TWO')");

    String jdbcSQL = "select * from test.XATT2";
    String xmlFile = TestUtil.getResourcesDir() + "/lib/checkQuery.xml";

    sqlExecuteVerify(null, new int[]{1}, jdbcSQL, xmlFile, "before_xa_commit");
    stm.execute("select * from test.XATT2");
    ResultSet r = stm.getResultSet();
    int cnt = 0;
    while (r.next()) {
      cnt++;
    }
    assertEquals(2, cnt);
    xaRes.end(xid, XAResource.TMSUCCESS);
    xaRes.prepare(xid);
    xaRes.commit(xid, false);
    VM servervm = getServerVM(1);
    servervm.invoke(TransactionDUnit.class, "waitForPendingCommit");
    TXManagerImpl.waitForPendingCommitForTest();

    sqlExecuteVerify(null, new int[]{1}, jdbcSQL, xmlFile, "after_xa_commit");
  }

  public void _testXATransactionFromClient_rollback() throws Exception {
    startServerVMs(2, 0, null);
    startClientVMs(1, 0, null);
    final int netport = startNetworkServer(1, null, null);
    serverSQLExecute(1, "create schema test");
    serverSQLExecute(1, "create table test.XATT2 (intcol int not null, text varchar(100) not null)" + getSuffix());
    serverSQLExecute(1, "insert into test.XATT2 values (1, 'ONE')");

    ClientXADataSource xaDataSource = (ClientXADataSource)TestUtil
        .getXADataSource(TestUtil.NetClientXADsClassName);
    byte[] gid = new byte[64];
    byte[] bid = new byte[64];
    for (int i = 0; i < 64; i++) {
      gid[i] = (byte)i;
      bid[i] = (byte)(64 - i);
    }
    Xid xid = new ClientXid(0x1234, gid, bid);

    String localhost = SocketCreator.getLocalHost().getHostName();
    xaDataSource.setServerName(localhost);
    xaDataSource.setPortNumber(netport);

    xaDataSource.setDatabaseName("gemfirexd");
    // get the stuff required to execute the global transaction
    XAConnection xaConn = xaDataSource.getXAConnection();
    XAResource xaRes = xaConn.getXAResource();
    Connection conn = xaConn.getConnection();

    // start the transaction with that xid
    xaRes.start(xid, XAResource.TMNOFLAGS);

    conn.setTransactionIsolation(getIsolationLevel());
    // do some work
    Statement stm = conn.createStatement();
    stm.execute("insert into test.XATT2 values (2, 'TWO')");

    String jdbcSQL = "select * from test.XATT2";
    String xmlFile = TestUtil.getResourcesDir() + "/lib/checkQuery.xml";

    sqlExecuteVerify(null, new int[]{1}, jdbcSQL, xmlFile, "before_xa_commit");

    stm.execute("select * from test.XATT2");
    ResultSet r = stm.getResultSet();
    int cnt = 0;
    while (r.next()) {
      cnt++;
    }
    assertEquals(2, cnt);
    xaRes.end(xid, XAResource.TMSUCCESS);
    xaRes.prepare(xid);
    xaRes.rollback(xid);

    VM servervm = getServerVM(1);
    servervm.invoke(TransactionDUnit.class, "waitForPendingCommit");
    sqlExecuteVerify(null, new int[]{1}, jdbcSQL, xmlFile, "before_xa_commit");
  }

  public void _testXATransactionFromPeerClient_rollback() throws Exception {
    startServerVMs(2, 0, null);
    startClientVMs(1, 0, null);

    serverSQLExecute(1, "create schema test");
    serverSQLExecute(1, "create table test.XATT2 (intcol int not null, text varchar(100) not null)" + getSuffix());
    serverSQLExecute(1, "insert into test.XATT2 values (1, 'ONE')");

    EmbeddedXADataSource xaDataSource = (EmbeddedXADataSource)TestUtil
        .getXADataSource(TestUtil.EmbeddedeXADsClassName);
    byte[] gid = new byte[64];
    byte[] bid = new byte[64];
    for (int i = 0; i < 64; i++) {
      gid[i] = (byte)i;
      bid[i] = (byte)(64 - i);
    }
    Xid xid = new ClientXid(0x1234, gid, bid);

    // get the stuff required to execute the global transaction
    XAConnection xaConn = xaDataSource.getXAConnection();
    XAResource xaRes = xaConn.getXAResource();
    Connection conn = xaConn.getConnection();

    // start the transaction with that xid
    xaRes.start(xid, XAResource.TMNOFLAGS);

    conn.setTransactionIsolation(getIsolationLevel());
    // do some work
    Statement stm = conn.createStatement();
    stm.execute("insert into test.XATT2 values (2, 'TWO')");

    String jdbcSQL = "select * from test.XATT2";
    String xmlFile = TestUtil.getResourcesDir() + "/lib/checkQuery.xml";

    sqlExecuteVerify(null, new int[]{1}, jdbcSQL, xmlFile, "before_xa_commit");
    stm.execute("select * from test.XATT2");
    ResultSet r = stm.getResultSet();
    int cnt = 0;
    while (r.next()) {
      cnt++;
    }
    assertEquals(2, cnt);
    xaRes.end(xid, XAResource.TMSUCCESS);
    xaRes.prepare(xid);
    xaRes.rollback(xid);
    //TXManagerImpl.waitForPendingCommitForTest();
    VM servervm = getServerVM(1);
    servervm.invoke(TransactionDUnit.class, "waitForPendingCommit");

    sqlExecuteVerify(null, new int[]{1}, jdbcSQL, xmlFile, "before_xa_commit");
  }

  public static void waitForPendingCommit() {
    TXManagerImpl.waitForPendingCommitForTest();
  }

  /**
   * Install an observer called during index maintenance.
   */
  public static void installIndexObserver(String name, TXId txId) {
    CheckIndexOperations checkIndex = new CheckIndexOperations(name, txId);
    GemFireXDQueryObserver old = GemFireXDQueryObserverHolder
        .setInstance(checkIndex);
    assertNull(old);
  }

  /**
   * Check index operations and reset the holder.
   */
  public static void checkIndexAndReset(int numInsertExpected,
      int numDeletesExpected) throws Exception {

    CheckIndexOperations checkIndex = GemFireXDQueryObserverHolder
        .<CheckIndexOperations>getObserver(CheckIndexOperations.class);
    checkIndex.checkNumInserts(numInsertExpected);
    checkIndex.checkNumDeletes(numDeletesExpected);
  }

  /**
   * Reset index observer.
   */
  public static void resetIndexObserver() throws Exception {

    GemFireXDQueryObserverHolder.clearInstance();
  }

  /**
   * Internal test class for checking index operations.
   *
   * @author rdubey
   */
  static class CheckIndexOperations extends GemFireXDQueryObserverAdapter {

    private static final long serialVersionUID = -549170799818823122L;

    int numInserts = 0;

    int numDeletes = 0;

    final String indexName;

    final TXId txId;

    CheckIndexOperations(String name, TXId txId) {
      this.indexName = name;
      this.txId = txId;
    }

    @Override
    public void keyAndContainerAfterLocalIndexInsert(Object key,
        Object rowLocation, GemFireContainer container) {

      if (checkTX()) {
        assertNotNull(key);
        assertNotNull(rowLocation);
        assertTrue(this.indexName.equalsIgnoreCase(container.getSchemaName()
            + "." + container.getTableName().toString()));
        this.numInserts++;
      }
    }

    @Override
    public void keyAndContainerAfterLocalIndexDelete(Object key,
        Object rowLocation, GemFireContainer container) {
      if (checkTX()) {
        assertNotNull(key);
        assertNotNull(rowLocation);
        assertTrue(this.indexName.equalsIgnoreCase(container.getSchemaName()
            + "." + container.getTableName().toString()));
        this.numDeletes++;
      }
    }

    private void checkNumInserts(int expected) {
      assertEquals(expected, this.numInserts);
    }

    private void checkNumDeletes(int expected) {
      assertEquals(expected, this.numDeletes);
    }

    private boolean checkTX() {
      return (this.txId == null || this.txId.equals(TXManagerImpl
          .getCurrentTXId()));
    }
  }

  public void _testBug41694() throws Exception {
    startServerVMs(2, 0, "sg1");
    startClientVMs(1, 0, null);
    Connection conn = getClientConnection(false);
    Statement st = conn.createStatement();
    st.execute("create schema test default server groups (sg1, sg2)");
    st.execute("create table test.t1 ( PkCol1 int not null, PkCol2 int not null, "
        + "col3 int, col4 int, col5 varchar(10),col6 int, col7 int, col8 int, "
        + "col9 int, col10 int, col11 int, col12 int, col13 int, col14 int, "
        + "col15 int, col16 int, col17 int, col18 int, col19 int, col20 int, "
        + "col21 int,col22 int, col23 int, col24 int, col25 int, col26 int, "
        + "col27 int, col28 int, col29 int, col30 int, col31 int, col32 int,"
        + " col33 int, col34 int, col35 int, col36 int, col37 int, col38 int, "
        + "col39 int, col40 int, col41 int, col42 int, col43 int, col44 int, "
        + "col45 int, col46 int, col47 int, col48 int, col49 int, col50 int, "
        + "col51 int, col52 int, col53 int, col54 int, col55 int, col56 int, "
        + "col57 int, col58 int, col59 int, col60 int, col61 int, col62 int, "
        + "col63 int, col64 int, col65 int, col66 int, col67 int, col68 int, "
        + "col69 int, col70 int, col71 int, col72 int, col73 int, col74 int, "
        + "col75 int, col76 int, col77 int, col78 int, col79 int, col80 int, "
        + "col81 int, col82 int, col83 int, col84 int, col85 int, col86 int, "
        + "col87 int, col88 int, col89 int, col90 int, col91 int, col92 int, "
        + "col93 int, col94 int, col95 int, col96 int, col97 int, col98 int, "
        + "col99 int, col100 int, Primary Key (PkCol1) ) "
        + "Partition by Primary Key server groups (sg1) redundancy 1" + getSuffix());
    conn.commit();
    st.execute("create index IndexCol4 on test.t1 (col4)");
    conn.commit();

    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    final int numRows = 1;
    PreparedStatement psInsert = conn.prepareStatement("insert into test.t1 "
        + "values(?, 1000, 1000, 1000, 'XXXX1'"
        + " , 1000, 1000, 1000, 1000, 1000"
        + " , 1000, 1000, 1000, 1000, 1000 "
        + " , 1000, 1000, 1000, 1000, 1000 "
        + " , 1000, 1000, 1000, 1000, 1000"
        + " , 1000, 1000, 1000, 1000, 1000"
        + " , 1000, 1000, 1000, 1000, 1000"
        + " , 1000, 1000, 1000, 1000, 1000"
        + " , 1000, 1000, 1000, 1000, 1000"
        + " , 1000, 1000, 1000, 1000, 1000"
        + " , 1000, 1000, 1000, 1000, 1000"
        + " , 1000, 1000, 1000, 1000, 1000"
        + " , 1000, 1000, 1000, 1000, 1000"
        + " , 1000, 1000, 1000, 1000, 1000"
        + " , 1000, 1000, 1000, 1000, 1000"
        + " , 1000, 1000, 1000, 1000, 1000"
        + " , 1000, 1000, 1000, 1000, 1000"
        + " , 1000, 1000, 1000, 1000, 1000"
        + " , 1000, 1000, 1000, 1000, 1000"
        + " , 1000, 1000, 1000, 1000, 1000 )");
    // st.execute("insert into test.t1 values(10, 10, 10, 10, 'XXXX1')");
    for (int i = 0; i < numRows; i++) {
      psInsert.setInt(1, i);
      psInsert.executeUpdate();
      conn.commit();
    }

    PreparedStatement psUpdate = conn.prepareStatement("update test.t1 set "
        + "col3 = 20 where PkCol1=?");
    // st.execute("update test.t1 set col3 = 20, col4 = 20, col5 = 'changed' where PkCol1=10");
    for (int i = 0; i < 1000; i++) {
      // Update the same row over and over should not cause #41694,
      // negative bucket size(memory consumed by bucket).
      psUpdate.setInt(1, 0);
      psUpdate.executeUpdate();
      conn.commit();
    }

    st.close();
    conn.commit();
  }

  /**
   * Simple test case of timing inserts.
   */
  public void _testUseCase_timeInserts() throws Exception {
    // reduce logs
    reduceLogLevelForTest("warning");

    //startVMs(0, 1);
    final int netPort = startNetworkServer(1, null, null);
    final Connection conn = TestUtil.getNetConnection(netPort,
        "/;user=app;password=app", null);
    Statement s = conn.createStatement();
    try {
      s.executeUpdate("drop table securities");
    } catch (Exception ignored) {
    }

    s.executeUpdate("create table securities ( sec_id integer not null "
        + "constraint sec_pk primary key, symbol varchar(30), "
        + "price double precision, exchange varchar(30),count integer)" + getSuffix());
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    getLogWriter().info(" Warming up ...");

    dumpIntoTable(conn, 1000);
    conn.rollback();
    long begin = System.nanoTime();
    dumpIntoTable(conn, 10000);
    conn.commit();
    long end = System.nanoTime();
    getLogWriter().info("Time to execute 10k operations"
        + (end - begin) / 1000000 + " millis");
  }

  protected void dumpIntoTable(Connection conn, int rows) throws SQLException {
    PreparedStatement ps = conn
        .prepareStatement("insert into securities values(?,?,?,?,?)");
    while (rows > 0) {
      ps.setInt(1, rows + 20000);
      ps.setString(2, "SYM" + String.valueOf(rows));
      ps.setFloat(3, 345);
      ps.setString(4, "nasdaq");
      ps.setInt(5, rows);
      ps.executeUpdate();
      rows--;
    }
  }

  /**
   * Test key based deletes in transaction, using p2p drive.
   *
   * @throws Exception
   */
  public void _testTransactionalKeyBasedDeletes() throws Exception {
    //startVMs(1, 1);
    Connection conn = getClientConnection(false);
    Statement st = conn.createStatement();
    st.execute("create schema tran");
    st.execute("Create table tran.t1 (c1 int not null , c2 int not null, "
        + "primary key(c1))" + getSuffix());
    conn.commit();
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    st.execute("insert into tran.t1 values (10, 10)");
    conn.commit();

    st.execute("delete from tran.t1 where c1 = 10");
    ResultSet rs = st.executeQuery("select * from tran.t1");
    assertFalse(rs.next());
    conn.commit();
    rs = st.executeQuery("select * from tran.t1");
    assertFalse(rs.next());
    conn.commit();
    rs.close();
    st.close();
    conn.close();
  }

  /**
   * Index maintenance with transactional deletes, p2p.
   *
   * @throws Exception
   */
  public void _testTransactionalDeleteWithLocalIndexes() throws Exception {
    //startVMs(1, 1);
    Properties props = new Properties();
    props.setProperty(Attribute.TX_SYNC_COMMITS, "true");
    final Connection conn = TestUtil.getConnection(props);
    Statement st = conn.createStatement();
    st.execute("create schema tran");
    st.execute("Create table tran.t1 (c1 int not null , c2 int not null, "
        + "primary key(c1))" + getSuffix());
    conn.commit();
    st.execute("create index IndexCol2 on tran.t1 (c2)");
    conn.commit();
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    final int numRows = 1000;
    PreparedStatement ps = conn.prepareStatement("insert into tran.t1 values "
        + "(?, ?)");
    for (int i = 0; i < numRows; i++) {
      ps.setInt(1, i);
      ps.setInt(2, i);
      ps.executeUpdate();
    }
    PreparedStatement psDelete = conn.prepareStatement("delete from tran.t1 "
        + "where c1 = ?");
    conn.commit();

    // GemFireXDQueryObserver old = null;
    VM server1 = this.serverVMs.get(0);
    // below check also forces a new TX to start for getCurrentTXId() call
    ResultSet rs = conn.createStatement().executeQuery(
        "select count(*) from tran.t1");
    assertTrue(rs.next());
    assertEquals(numRows, rs.getInt(1));
    assertFalse(rs.next());
    server1.invoke(getClass(), "installIndexObserver",
        new Object[]{"tran.IndexCol2", TXManagerImpl.getCurrentTXId()});
    try {
      // old = GemFireXDQueryObserverHolder.setInstance(checkIndex);
      for (int i = 0; i < numRows; i++) {
        psDelete.setInt(1, i);
        psDelete.executeUpdate();
      }
      conn.commit();
      // checkIndex.checkNumDeletes(numRows);
    } finally {
      // if (old != null ) {
      // GemFireXDQueryObserverHolder.setInstance(old);
      // } else {
      // GemFireXDQueryObserverHolder.clearInstance();
      // }
    }
    server1.invoke(getClass(), "checkIndexAndReset",
        new Object[]{Integer.valueOf(0), Integer.valueOf(numRows)});
    server1.invoke(getClass(), "resetIndexObserver");

    st.close();
    conn.close();
  }

  /**
   * Index maintenance with transactional deletes, Client-Server.
   *
   * @throws Exception
   */
  public void _testTransactionalDeleteWithLocalIndexesClientServer()
      throws Exception {
    // startServerVMs(1);
    final int netPort = startNetworkServer(1, null, null);
    final Connection conn = TestUtil.getNetConnection(netPort,
        "/;user=app;password=app", null);
    Statement st = conn.createStatement();
    st.execute("create schema tran");
    st.execute("Create table tran.t1 (c1 int not null , c2 int not null, "
        + "primary key(c1))" + getSuffix());
    conn.commit();
    st.execute("create index IndexCol2 on tran.t1 (c2)");
    conn.commit();
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    int numRows = 1000;
    PreparedStatement ps = conn.prepareStatement("insert into tran.t1 values "
        + "(?, ?)");
    for (int i = 0; i < numRows; i++) {
      ps.setInt(1, i);
      ps.setInt(2, i);
      ps.executeUpdate();
      conn.commit();
    }

    // GemFireXDQueryObserver old = null;
    VM server1 = this.serverVMs.get(0);
    server1.invoke(getClass(), "installIndexObserver",
        new Object[]{"tran.IndexCol2", null});
    PreparedStatement psDelete = conn.prepareStatement("delete from tran.t1 "
        + "where c1 = ?");
    // old = GemFireXDQueryObserverHolder.setInstance(checkIndex);
    for (int i = 0; i < numRows; i++) {
      psDelete.setInt(1, i);
      psDelete.executeUpdate();
      conn.commit();
    }

    server1.invoke(getClass(), "checkIndexAndReset",
        new Object[]{Integer.valueOf(0), Integer.valueOf(numRows)});
    server1.invoke(getClass(), "resetIndexObserver");
    st.execute("drop table tran.t1");
    st.execute("drop schema tran restrict");

    st.close();
    conn.commit();
    conn.close();
  }

  /**
   * Index maintenance with transactional deletes, Client-Server, replicated
   * tables.
   *
   * @throws Exception
   */
  public void _testTransactionalDeleteWithLocalIndexesClientServerReplicatedTable()
      throws Exception {
    // startServerVMs(1);
    final int netPort = startNetworkServer(1, null, null);
    final Connection conn = TestUtil.getNetConnection(netPort,
        "/;user=app;password=app", null);
    Statement st = conn.createStatement();
    st.execute("create schema tran");
    st.execute("Create table tran.t1 (c1 int not null , c2 int not null, "
        + "primary key(c1)) replicate" + getSuffix());
    conn.commit();
    st.execute("create index IndexCol2 on tran.t1 (c2)");
    conn.commit();
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);

    VM server1 = this.serverVMs.get(0);
    server1.invoke(getClass(), "installIndexObserver",
        new Object[]{"tran.IndexCol2", null});

    int numRows = 1000;
    PreparedStatement ps = conn.prepareStatement("insert into tran.t1 values "
        + "(?, ?)");
    for (int i = 0; i < numRows; i++) {
      ps.setInt(1, i);
      ps.setInt(2, i);
      ps.executeUpdate();
      conn.commit();
    }

    server1.invoke(getClass(), "checkIndexAndReset",
        new Object[]{Integer.valueOf(numRows), Integer.valueOf(0)});

    // final CheckIndexOperations checkIndex = new
    // CheckIndexOperations("Tran.IndexCol2");
    // GemFireXDQueryObserver old = null;

    PreparedStatement psDelete = conn.prepareStatement("delete from tran.t1 "
        + "where c1 = ?");
    try {
      // old = GemFireXDQueryObserverHolder.setInstance(checkIndex);
      for (int i = 0; i < numRows; i++) {
        psDelete.setInt(1, i);
        psDelete.executeUpdate();
        conn.commit();
      }

    } finally {
    }

    server1.invoke(getClass(), "checkIndexAndReset",
        new Object[]{Integer.valueOf(numRows), Integer.valueOf(numRows)});
    server1.invoke(getClass(), "resetIndexObserver");

    st.execute("drop table tran.t1");
    st.execute("drop schema tran restrict");

    st.close();
    conn.commit();
    conn.close();
  }

  /**
   * Update throw function execution.
   *
   * @throws Exception
   */
  public void _testUpdateWithFunctionExecution() throws Exception {
    //startVMs(1, 2);
    Connection conn = getClientConnection(false);
    Statement st = conn.createStatement();
    st.execute("create schema trade");

    st.execute("create table trade.securities (sec_id int not null, "
        + "symbol varchar(10) not null, price decimal (30, 20), "
        + "exchange varchar(10) not null, tid int, "
        + "constraint sec_pk primary key (sec_id), "
        + "constraint sec_uq unique (symbol, exchange), "
        + "constraint exc_ch check (exchange in ('nasdaq', 'nye', 'amex', "
        + "'lse', 'fse', 'hkse', 'tse')))  partition by list (tid) "
        + "(VALUES (3, 15, 2, 7), VALUES (10, 16, 4, 5), "
        + "VALUES (1, 13, 6, 12), VALUES (0, 8, 9, 11, 14, 17)) " + getSuffix());

    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    final int numRows = 10;
    PreparedStatement ps = conn
        .prepareStatement("insert into trade.securities values "
            + "(?, ?, ?, ?, ?)");
    for (int i = 0; i < numRows; i++) {
      ps.setInt(1, i);
      ps.setString(2, "XXXX" + i);
      ps.setDouble(3, i);
      ps.setString(4, "nasdaq");
      ps.setInt(5, i);
      ps.executeUpdate();
    }
    conn.commit();

    InternalDistributedSystem.getAnyInstance().getLogWriter()
        .info("XXXX starting the update");
    PreparedStatement psUpdate = conn
        .prepareStatement("update trade.securities "
            + "set symbol = ? where sec_id = ? and tid = ?");
    for (int i = 0; i < numRows; i++) {
      psUpdate.setString(1, "YYYY" + i);
      psUpdate.setInt(2, i);
      psUpdate.setInt(3, i);
      psUpdate.executeUpdate();
    }
    // psUpdate.executeUpdate();
    InternalDistributedSystem.getAnyInstance().getLogWriter()
        .info("XXXX update is done");
    ResultSet rs = st
        .executeQuery("select * from trade.securities order by symbol");
    for (int i = 0; i < numRows; i++) {
      assertTrue(rs.next());
      assertEquals("YYYY" + i, rs.getString("SYMBOL"));
    }
    assertFalse(rs.next());
    conn.commit();

    rs = st.executeQuery("select * from trade.securities order by symbol");
    for (int i = 0; i < numRows; i++) {
      assertTrue(rs.next());
      assertEquals("YYYY" + i, rs.getString("SYMBOL"));
    }
    assertFalse(rs.next());
    conn.commit();

    rs.close();
    st.close();
    psUpdate.close();
    ps.close();
    conn.close();
  }

  /**
   * Multirow insert in transaction.
   *
   * @throws Exception
   */
  public void _testNetworkFailedDDLFollowedByInsert() throws Exception {
    // start three network servers
    startServerVMs(1, 0, null);
    final int netPort = startNetworkServer(1, null, null);
    final Connection conn = TestUtil.getNetConnection(netPort,
        "/;user=q;password=q", null);
    conn.commit();
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    Statement st = conn.createStatement();
    st.execute("create table t1 (PkCol1 int not null, Col2 int not null,"
        + " Primary Key (PkCol1))" + getSuffix());

    final int numRows = 3;
    st.execute("insert into t1 values (0,0), (1,1), (2,2)");

    ResultSet rs = st.executeQuery("select * from t1");
    for (int i = 0; i < numRows; i++) {
      assertTrue(rs.next());
    }
    assertFalse(rs.next());
    conn.commit();
    rs = st.executeQuery("select * from t1");
    for (int i = 0; i < numRows; i++) {
      assertTrue(rs.next());
    }
    assertFalse(rs.next());
    conn.commit();
    rs.close();

    st.execute("drop table t1");
    st.close();
    conn.commit();
    conn.close();
  }

  public void _test41679() throws Exception {
    //startVMs(1, 2);
    Connection conn = getClientConnection(false);
    Statement st = conn.createStatement();
    st.execute("create schema trade");
    st.execute("create table trade.customers (cid int not null, cust_name "
        + "varchar(100), since date, addr varchar(100), tid int, "
        + "primary key (cid))" + getSuffix());
    conn.commit();
    conn.setAutoCommit(false);
    conn.setTransactionIsolation(getIsolationLevel());
    PreparedStatement psInsert = conn.prepareStatement("insert into "
        + "trade.customers values(?, ?, ?, ?, ?)");
    for (int i = 0; i < 131; i++) {
      psInsert.setInt(1, i);
      psInsert.setString(2, "YYYY");
      psInsert.setDate(3, new java.sql.Date(System.currentTimeMillis()));
      psInsert.setString(4, "YYYY");
      psInsert.setInt(5, i);
      psInsert.executeUpdate();
    }
    conn.commit();
    PreparedStatement psUpdate = conn
        .prepareStatement("update trade.customers "
            + "set cust_name = ? where cid = ?");

    for (int i = 0; i < 131; i++) {
      try {
        psUpdate.setString(1, "XXXX");
        psUpdate.setInt(2, i);
        psUpdate.executeUpdate();
      } catch (SQLException e) {
        if (!"X0Z06".equals(e.getSQLState())) {
          throw e;
        }
        Throwable t = e.getCause();
        while (t.getCause() != null) {
          t = t.getCause();
        }
        assertTrue("unexpected cause: " + t, t instanceof EntryExistsException);
      }
    }
    ResultSet rs = st.executeQuery("select * from trade.customers");
    int numRow = 0;
    while (rs.next()) {
      numRow++;
    }
    System.out.println("XXXX num rows = " + numRow);
    rs.close();
    st.close();
    conn.commit();
    conn.close();
  }

  /**
   * Unique constraint violation on remote node.
   */
  public void _testUniquenessFailure() throws Exception {
    //startVMs(1, 1);
    Connection conn = getClientConnection(false);
    Statement st = conn.createStatement();
    st.execute("create schema tran");
    st.execute("Create table tran.t1 (c1 int not null , c2 int not null, "
        + "c3 int not null, primary key(c1), "
        + "constraint C3_Unique unique (c3)) " + getSuffix());// replicate");
    conn.commit();
    st.execute("create index IndexCol2 on tran.t1 (c2)");
    conn.commit();
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    int numRows = 2;
    PreparedStatement ps = conn.prepareStatement("insert into tran.t1 values "
        + "(?, ?, ?)");
    try {
      for (int i = 0; i < numRows; i++) {
        ps.setInt(1, i);
        ps.setInt(2, i);
        ps.setInt(3, 2);
        ps.executeUpdate();
      }
      fail("update should throw an exception for unique key violation");
    } catch (SQLException ex) {
      if (!"23505".equals(ex.getSQLState())) {
        throw ex;
      }
      Throwable t = ex.getCause();
      while (t.getCause() != null) {
        t = t.getCause();
      }
      assertTrue("unexpected cause: " + t, t instanceof EntryExistsException);
    }

    String goldenTextFile = TestUtil.getResourcesDir()
        + "/lib/checkQuery.xml";

    // constraint violation will cause the whole TX to be rolled back (#43170)
    ps.setInt(1, 0);
    ps.setInt(2, 0);
    ps.setInt(3, 2);
    ps.executeUpdate();
    ps.setInt(1, 4);
    ps.setInt(2, 5);
    ps.setInt(3, 6);
    ps.executeUpdate();

    String jdbcSQL = "select * from tran.t1";

    sqlExecuteVerify(new int[]{1}, null, jdbcSQL, goldenTextFile,
        "tran_uniq");

    conn.commit();
    sqlExecuteVerify(new int[]{1}, null, jdbcSQL, goldenTextFile,
        "tran_uniq");

    st.close();
    ps.close();
    conn.commit();
    conn.close();
  }

  /**
   * In doubt transaction because of uniqueness violation on remote node.
   *
   * @throws Exception
   */
  public void _testUniquenessFailureReplicatedTable() throws Exception {
    //startVMs(1, 1);
    Connection conn = getClientConnection(false);
    Statement st = conn.createStatement();
    st.execute("create schema tran");
    st.execute("Create table tran.t1 (c1 int not null , c2 int not null, c3 int not null,"
        + "primary key(c1), constraint C3_Unique unique (c3)) replicate" + getSuffix());
    conn.commit();
    st.execute("create index IndexCol2 on tran.t1 (c2)");
    conn.commit();
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    int numRows = 2;
    PreparedStatement ps = conn.prepareStatement("insert into tran.t1 values "
        + "(?, ?, ?)");
    try {
      for (int i = 0; i < numRows; i++) {
        ps.setInt(1, i);
        ps.setInt(2, i);
        ps.setInt(3, 2);
        ps.executeUpdate();
      }
      fail("The control should not reach here as "
          + "unique index key violation should be encountered during transaction op only");
      conn.commit();
    } catch (SQLException ex) {
      if (!"23505".equals(ex.getSQLState())) {
        throw ex;
      }
      Throwable t = ex.getCause();
      while (t.getCause() != null) {
        t = t.getCause();
      }
      assertTrue("unexpected cause: " + t, t instanceof EntryExistsException);
    }
    st.close();
    ps.close();
    conn.rollback();
    conn.close();
  }

  public void _testNonKeyBasedTransactionalUpdates() throws Exception {
    //startVMs(1, 1);
    Connection conn = getClientConnection(false);
    Statement st = conn.createStatement();
    st.execute("create schema trade");

    st.execute("create table trade.securities (sec_id int not null, "
        + "symbol varchar(10) not null, price decimal (30, 20), "
        + "exchange varchar(10) not null, tid int, "
        + "constraint sec_pk primary key (sec_id) ) "
        + " partition by column (tid) " + getSuffix());

    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    final int numRows = 5;
    PreparedStatement ps = conn
        .prepareStatement("insert into trade.securities values "
            + "(?, ?, ?, ?, ?)");
    for (int i = 0; i < numRows; i++) {
      ps.setInt(1, i);
      ps.setString(2, "XXXX" + i);
      ps.setDouble(3, i);
      ps.setString(4, "nasdaq");
      ps.setInt(5, i);
      ps.executeUpdate();
    }
    conn.commit();

    PreparedStatement psUpdate = conn
        .prepareStatement("update trade.securities "
            + "set symbol = ? where sec_id = ? and tid = ?");
    for (int i = 0; i < numRows; i++) {
      psUpdate.setString(1, "YYY" + i);
      psUpdate.setInt(2, i);
      psUpdate.setInt(3, i);
      psUpdate.executeUpdate();
    }
    // psUpdate.executeUpdate();
    // InternalDistributedSystem.getAnyInstance().getLogWriter().info("XXXX update is done");
    ResultSet rs = st.executeQuery("select * from trade.securities");
    int numRowsReturned = 0;
    while (rs.next()) {
      assertTrue("Got" + rs.getString("SYMBOL").trim(),
          (rs.getString("SYMBOL").trim()).startsWith("YYY"));
      numRowsReturned++;
    }
    assertEquals("Expected " + numRows + " row but found " + numRowsReturned,
        numRows, numRowsReturned);
    conn.commit();

    rs = st.executeQuery("select * from trade.securities");
    // assertTrue(rs.next());
    int numUpdates = 0;
    while (rs.next()) {
      assertTrue("Got " + rs.getString("SYMBOL").trim(), rs.getString("SYMBOL")
          .trim().startsWith("YYY"));
      numUpdates++;
    }
    assertEquals(numRows, numUpdates);
    conn.commit();
    rs.close();
    st.close();
    psUpdate.close();
    ps.close();
    conn.close();
  }

  public void _testNonKeyBasedTransactionalUpdatesRollbackAndCommit()
      throws Exception {
    //startVMs(1, 1);
    Connection conn = getClientConnection(false);
    Statement st = conn.createStatement();
    st.execute("create schema trade");

    st.execute("create table trade.securities (sec_id int not null, "
        + "symbol varchar(10) not null, price decimal (30, 20), "
        + "exchange varchar(10) not null, tid int, "
        + "constraint sec_pk primary key (sec_id) ) "
        + " partition by column (tid) " + getSuffix());

    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);

    final int numRows = 5;
    PreparedStatement ps = conn
        .prepareStatement("insert into trade.securities values "
            + "(?, ?, ?, ?, ?)");
    for (int i = 0; i < numRows; i++) {
      ps.setInt(1, i);
      ps.setString(2, "XXXX" + i);
      ps.setDouble(3, i);
      ps.setString(4, "nasdaq");
      ps.setInt(5, i);
      ps.executeUpdate();
    }
    conn.commit();

    PreparedStatement psUpdate = conn
        .prepareStatement("update trade.securities "
            + "set symbol = ? where sec_id = ? and tid = ?");
    for (int i = 0; i < numRows; i++) {
      psUpdate.setString(1, "YYY" + i);
      psUpdate.setInt(2, i);
      psUpdate.setInt(3, i);
      psUpdate.executeUpdate();
    }
    // psUpdate.executeUpdate();
    // InternalDistributedSystem.getAnyInstance().getLogWriter().info("XXXX update is done");
    ResultSet rs = st.executeQuery("select * from trade.securities");
    int numRowsReturned = 0;
    while (rs.next()) {
      assertTrue("Got" + rs.getString("SYMBOL").trim(),
          (rs.getString("SYMBOL").trim()).startsWith("YYY"));
      numRowsReturned++;
    }
    assertEquals("Expected " + numRows + " row but found " + numRowsReturned,
        numRows, numRowsReturned);
    conn.rollback();
    // now commit, should be an empty tran.
    conn.commit();
    rs = st.executeQuery("select * from trade.securities");

    int numUpdates = 0;
    while (rs.next()) {
      assertTrue("Got " + rs.getString("SYMBOL").trim(), rs.getString("SYMBOL")
          .trim().startsWith("XXXX"));
      numUpdates++;
    }
    assertEquals(numRows, numUpdates);
    rs.close();

    for (int i = 0; i < numRows; i++) {
      psUpdate.setString(1, "YYY" + i);
      psUpdate.setInt(2, i);
      psUpdate.setInt(3, i);
      psUpdate.executeUpdate();
    }
    conn.commit();

    // verify.
    rs = st.executeQuery("select * from trade.securities");
    numUpdates = 0;
    while (rs.next()) {
      assertTrue("Got " + rs.getString("SYMBOL").trim(), rs.getString("SYMBOL")
          .trim().startsWith("YYY"));
      numUpdates++;
    }
    assertEquals(numRows, numUpdates);

    conn.commit();
    rs.close();
    st.close();
    psUpdate.close();
    ps.close();
    conn.close();
  }

  public void _testNonKeyBasedTransactionalUpdatesAndConflict() throws Exception {
    //startVMs(1, 1);
    Connection conn = getClientConnection(false);
    Statement st = conn.createStatement();
    st.execute("create schema trade");

    st.execute("create table trade.securities (sec_id int not null, "
        + "symbol varchar(10) not null, price decimal (30, 20), "
        + "exchange varchar(10) not null, tid int, "
        + "constraint sec_pk primary key (sec_id) ) "
        + " partition by primary key" + getSuffix());// column (tid) ");

    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);

    final int numRows = 5;
    PreparedStatement ps = conn
        .prepareStatement("insert into trade.securities values "
            + "(?, ?, ?, ?, ?)");
    for (int i = 0; i < numRows; i++) {
      ps.setInt(1, i);
      ps.setString(2, "XXXX" + i);
      ps.setDouble(3, i);
      ps.setString(4, "nasdaq");
      ps.setInt(5, i);
      ps.executeUpdate();
    }
    conn.commit();

    PreparedStatement psUpdate = conn
        .prepareStatement("update trade.securities "
            + "set symbol = ? where sec_id = ? and tid = ?");
    for (int i = 0; i < numRows; i++) {
      psUpdate.setString(1, "YYY" + i);
      psUpdate.setInt(2, i);
      psUpdate.setInt(3, i);
      psUpdate.executeUpdate();
    }

    this.gotConflict = false;
    Thread otherTx = new Thread(new Runnable() {
      @Override
      public void run() {
        final Region<Object, Object> r = Misc
            .getRegionForTable("TRADE.SECURITIES", true);
        final CacheTransactionManager txManager = Misc.getGemFireCache()
            .getCacheTransactionManager();
        txManager.begin();
        try {
          Object key = TestUtil.getGemFireKey(0, r);
          Object row = r.get(key);
          r.put(key, row);
        } catch (ConflictException ce) {
          gotConflict = true;
          txManager.rollback();
        } catch (StandardException se) {
          gotConflict = false;
          fail("failing put with exception", se);
        }
      }
    });
    otherTx.start();
    otherTx.join();

    assertTrue("expected conflict", this.gotConflict);
    this.gotConflict = false;

    conn.commit();
    // rs.close();
    st.close();
    psUpdate.close();
    ps.close();
    conn.close();
  }

  public void _testBulkTransactionalUpdatesRollbackAndCommitClientServer()
      throws Exception {

    final int netPort = startNetworkServer(1, null, null);
    final Connection conn = TestUtil.getNetConnection(netPort,
        "/;user=app;password=app", null);
    final Connection conn2 = TestUtil.getNetConnection(netPort,
        "/;user=app;password=app", null);
    Statement st = conn.createStatement();
    st.execute("create schema trade");

    st.execute("create table trade.securities (sec_id int not null, "
        + "symbol varchar(10) not null, price decimal (30, 20), "
        + "exchange varchar(10) not null, tid int, "
        + "constraint sec_pk primary key (sec_id) ) "
        + "partition by column (tid) " + getSuffix());

    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    final int numRows = 5;
    PreparedStatement ps = conn.prepareStatement("insert into "
        + "trade.securities values (?, ?, ?, ?, ?)");
    for (int i = 0; i < numRows; i++) {
      ps.setInt(1, i);
      ps.setString(2, "XXXX" + i);
      ps.setDouble(3, i);
      ps.setString(4, "nasdaq");
      ps.setInt(5, i);
      ps.executeUpdate();
    }
    conn.commit();
    PreparedStatement psUpdate = conn.prepareStatement("update "
        + "trade.securities set symbol = ? where sec_id = ? and tid = ?");
    for (int i = 0; i < numRows; i++) {
      psUpdate.setString(1, "YYY" + i);
      psUpdate.setInt(2, i);
      psUpdate.setInt(3, i);
      psUpdate.executeUpdate();
    }
    ResultSet rs = st.executeQuery("select * from trade.securities");
    int numRowsReturned = 0;
    // same transaction should see the updates
    while (rs.next()) {
      assertTrue("Got " + rs.getString("SYMBOL").trim(),
          (rs.getString("SYMBOL").trim()).startsWith("YYY"));
      numRowsReturned++;
    }
    assertEquals("Expected " + numRows + " row but found " + numRowsReturned,
        numRows, numRowsReturned);
    // different connection should not see the updates
    Statement st2 = conn2.createStatement();
    rs = st2.executeQuery("select * from trade.securities");
    numRowsReturned = 0;
    // same transaction should see the updates
    while (rs.next()) {
      assertFalse("Got " + rs.getString("SYMBOL").trim(),
          (rs.getString("SYMBOL").trim()).startsWith("YYY"));
      assertTrue("Got " + rs.getString("SYMBOL").trim(),
          (rs.getString("SYMBOL").trim()).startsWith("XXX"));
      numRowsReturned++;
    }
    assertEquals("Expected " + numRows + " row but found " + numRowsReturned,
        numRows, numRowsReturned);
    conn.rollback();

    // now commit, should be an empty tran.
    conn.commit();
    rs = st.executeQuery("select * from trade.securities");
    int numUpdates = 0;
    while (rs.next()) {
      assertTrue("Got " + rs.getString("SYMBOL").trim(), rs.getString("SYMBOL")
          .trim().startsWith("XXXX"));
      numUpdates++;
    }
    assertEquals(numRows, numUpdates);
    rs.close();

    rs = st2.executeQuery("select * from trade.securities");
    numUpdates = 0;
    while (rs.next()) {
      assertTrue("Got " + rs.getString("SYMBOL").trim(), rs.getString("SYMBOL")
          .trim().startsWith("XXXX"));
      numUpdates++;
    }
    assertEquals(numRows, numUpdates);
    rs.close();

    for (int i = 0; i < numRows; i++) {
      psUpdate.setString(1, "YYY" + i);
      psUpdate.setInt(2, i);
      psUpdate.setInt(3, i);
      psUpdate.executeUpdate();
    }
    conn.commit();

    // verify.
    rs = st.executeQuery("select * from trade.securities");
    numUpdates = 0;
    while (rs.next()) {
      assertTrue("Got " + rs.getString("SYMBOL").trim(), rs.getString("SYMBOL")
          .trim().startsWith("YYY"));
      numUpdates++;
    }
    assertEquals(numRows, numUpdates);
    rs = st2.executeQuery("select * from trade.securities");
    numUpdates = 0;
    while (rs.next()) {
      assertTrue("Got " + rs.getString("SYMBOL").trim(), rs.getString("SYMBOL")
          .trim().startsWith("YYY"));
      numUpdates++;
    }
    assertEquals(numRows, numUpdates);

    conn.commit();
    conn2.commit();
    rs.close();
    st.close();
    st2.close();
    psUpdate.close();
    ps.close();
    conn.close();
    conn2.close();
  }

  @SuppressWarnings({"unchecked", "deprecation"})
  public static <V, K> Region<K, V> createRegion() throws Exception {
    AttributesFactory<K, V> af = new AttributesFactory<K, V>();
    // af.setScope(Scope.DISTRIBUTED_NO_ACK);
    af.setIndexMaintenanceSynchronous(true);
    af.setDataPolicy(DataPolicy.PARTITION);
    af.setPartitionAttributes(new PartitionAttributesFactory<K, V>()
        .setLocalMaxMemory(0).create());
    Region<K, V> region = Misc.getGemFireCache().createRegion("TXTest",
        af.create());
    return region;
  }

  @SuppressWarnings({"rawtypes", "deprecation"})
  public static Region createRegionStore() throws Exception {
    Region r = Misc.getGemFireCache().getRegion("TXTest");
    if (r != null) {
      return null;
    }
    AttributesFactory af = new AttributesFactory();
    // af.setScope(Scope.DISTRIBUTED_NO_ACK);
    af.setIndexMaintenanceSynchronous(true);
    af.setDataPolicy(DataPolicy.PARTITION);
    af.setPartitionAttributes(new PartitionAttributesFactory()
        .setLocalMaxMemory(10).create());
    Region region = Misc.getGemFireCache().createRegion("TXTest", af.create());
    return region;
  }

  public void _testBulkTransactionalUpdatesRollbackAndCommitClientServerReplicateTable()
      throws Exception {

    final int netPort = startNetworkServer(1, null, null);
    final Connection conn = TestUtil.getNetConnection(netPort,
        "/;user=app;password=app", null);
    Statement st = conn.createStatement();
    st.execute("create schema trade");

    st.execute("create table trade.securities (sec_id int not null, "
        + "symbol varchar(10) not null, price decimal (30, 20), "
        + "exchange varchar(10) not null, tid int, "
        + "constraint sec_pk primary key (sec_id) ) " + " replicate " + getSuffix());

    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    final int numRows = 5;
    PreparedStatement ps = conn
        .prepareStatement("insert into trade.securities values "
            + "(?, ?, ?, ?, ?)");
    for (int i = 0; i < numRows; i++) {
      ps.setInt(1, i);
      ps.setString(2, "XXXX" + i);
      ps.setDouble(3, i);
      ps.setString(4, "nasdaq");
      ps.setInt(5, i);
      ps.executeUpdate();
    }
    conn.commit();

    PreparedStatement psUpdate = conn
        .prepareStatement("update trade.securities "
            + "set symbol = ? where sec_id = ? and tid = ?");
    for (int i = 0; i < numRows; i++) {
      psUpdate.setString(1, "YYY" + i);
      psUpdate.setInt(2, i);
      psUpdate.setInt(3, i);
      psUpdate.executeUpdate();
    }
    // psUpdate.executeUpdate();
    // InternalDistributedSystem.getAnyInstance().getLogWriter().info("XXXX update is done");
    ResultSet rs = st.executeQuery("select * from trade.securities");
    int numRowsReturned = 0;
    while (rs.next()) {
      assertTrue("Got" + rs.getString("SYMBOL").trim(),
          (rs.getString("SYMBOL").trim()).startsWith("YYY"));
      numRowsReturned++;
    }
    assertEquals("Expected " + numRows + " row but found " + numRowsReturned,
        numRows, numRowsReturned);
    conn.rollback();
    // now commit, should be an empty tran.
    conn.commit();
    rs = st.executeQuery("select * from trade.securities");

    int numUpdates = 0;
    while (rs.next()) {
      assertTrue("Got " + rs.getString("SYMBOL").trim(), rs.getString("SYMBOL")
          .trim().startsWith("XXXX"));
      numUpdates++;
    }
    assertEquals(numRows, numUpdates);
    rs.close();

    for (int i = 0; i < numRows; i++) {
      psUpdate.setString(1, "YYY" + i);
      psUpdate.setInt(2, i);
      psUpdate.setInt(3, i);
      psUpdate.executeUpdate();
    }
    conn.commit();

    // verify.
    rs = st.executeQuery("select * from trade.securities");
    numUpdates = 0;
    while (rs.next()) {
      assertTrue("Got " + rs.getString("SYMBOL").trim(), rs.getString("SYMBOL")
          .trim().startsWith("YYY"));
      numUpdates++;
    }
    assertEquals(numRows, numUpdates);

    conn.commit();
    rs.close();
    st.close();
    psUpdate.close();
    ps.close();
    conn.close();
  }

  public void _testNonKeyBasedTransactionalUpdatesRollbackAndCommitReplicateTable()
      throws Exception {
    //startVMs(1, 1);
    Connection conn = getClientConnection(false);
    Statement st = conn.createStatement();
    st.execute("create schema trade");

    st.execute("create table trade.securities (sec_id int not null, "
        + "symbol varchar(10) not null, price decimal (30, 20), "
        + "exchange varchar(10) not null, tid int, "
        + "constraint sec_pk primary key (sec_id) ) " + " replicate " + getSuffix());
    conn.commit();
    conn.setAutoCommit(false);

    conn.setTransactionIsolation(getIsolationLevel());
    final int numRows = 5;
    PreparedStatement ps = conn
        .prepareStatement("insert into trade.securities values "
            + "(?, ?, ?, ?, ?)");
    for (int i = 0; i < numRows; i++) {
      ps.setInt(1, i);
      ps.setString(2, "XXXX" + i);
      ps.setDouble(3, i);
      ps.setString(4, "nasdaq");
      ps.setInt(5, i);
      ps.executeUpdate();
    }
    conn.commit();

    PreparedStatement psUpdate = conn
        .prepareStatement("update trade.securities "
            + "set symbol = ? where sec_id = ? and tid = ?");
    for (int i = 0; i < numRows; i++) {
      psUpdate.setString(1, "YYY" + i);
      psUpdate.setInt(2, i);
      psUpdate.setInt(3, i);
      psUpdate.executeUpdate();
    }
    // psUpdate.executeUpdate();
    // InternalDistributedSystem.getAnyInstance().getLogWriter().info("XXXX update is done");
    ResultSet rs = st.executeQuery("select * from trade.securities");
    int numRowsReturned = 0;
    while (rs.next()) {
      assertTrue("Got" + rs.getString("SYMBOL").trim(),
          (rs.getString("SYMBOL").trim()).startsWith("YYY"));
      numRowsReturned++;
    }
    assertEquals("Expected " + numRows + " row but found " + numRowsReturned,
        numRows, numRowsReturned);
    conn.rollback();
    // now commit, should be an empty tran.
    conn.commit();
    rs = st.executeQuery("select * from trade.securities");

    int numUpdates = 0;
    while (rs.next()) {
      assertTrue("Got " + rs.getString("SYMBOL").trim(), rs.getString("SYMBOL")
          .trim().startsWith("XXXX"));
      numUpdates++;
    }
    assertEquals(numRows, numUpdates);
    rs.close();

    for (int i = 0; i < numRows; i++) {
      psUpdate.setString(1, "YYY" + i);
      psUpdate.setInt(2, i);
      psUpdate.setInt(3, i);
      psUpdate.executeUpdate();
    }
    conn.commit();

    // verify.
    rs = st.executeQuery("select * from trade.securities");
    numUpdates = 0;
    while (rs.next()) {
      assertTrue("Got " + rs.getString("SYMBOL").trim(), rs.getString("SYMBOL")
          .trim().startsWith("YYY"));
      numUpdates++;
    }
    assertEquals(numRows, numUpdates);

    conn.commit();
    rs.close();
    st.close();
    psUpdate.close();
    ps.close();
    conn.close();
  }

  public void _testBug41970_43473() throws Throwable {
    //startVMs(1, 1);
    Connection conn = getClientConnection(false);
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    Statement st = conn.createStatement();
    st.execute("create table customers (cid int not null, cust_name "
        + "varchar(100),  addr varchar(100), tid int, primary key (cid))");
    st.execute("create table trades (tid int, cid int, eid int, primary Key "
        + "(tid), foreign key (cid) references customers (cid))" + getSuffix());
    PreparedStatement pstmt = conn
        .prepareStatement("insert into customers values(?,?,?,?)");
    pstmt.setInt(1, 1);
    pstmt.setString(2, "name1");
    pstmt.setString(3, "add1");
    pstmt.setInt(4, 1);
    pstmt.executeUpdate();
    pstmt.setInt(1, 2);
    pstmt.setString(2, "name2");
    pstmt.setString(3, "add2");
    pstmt.setInt(4, 1);
    pstmt.executeUpdate();
    conn.commit();

    ResultSet rs = st.executeQuery("Select * from customers");
    int numRows = 0;
    while (rs.next()) {
      // Checking number of rows returned, since ordering of results
      // is not guaranteed.
      numRows++;
    }
    assertEquals("ResultSet should contain two rows ", 2, numRows);
    rs.close();
    conn.commit();

    // test for #43473
    st.execute("create table sellorders (oid int not null primary key, "
        + "cid int, order_time timestamp, status varchar(10), "
        + "constraint ch check (status in ('cancelled', 'open', 'filled')))" + getSuffix());
    pstmt = conn.prepareStatement("insert into sellorders values (?, ?, ?, ?)");
    final long currentTime = System.currentTimeMillis();
    final Timestamp ts = new Timestamp(currentTime - 100);
    final Timestamp now = new Timestamp(currentTime);
    for (int id = 1; id <= 100; id++) {
      pstmt.setInt(1, id);
      pstmt.setInt(2, id * 2);
      pstmt.setTimestamp(3, ts);
      pstmt.setString(4, "open");
      pstmt.execute();
    }
    conn.commit();

    final CyclicBarrier barrier = new CyclicBarrier(2);
    final Throwable[] failure = new Throwable[1];
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Connection conn2 = getClientConnection(false);
          conn2.setTransactionIsolation(getIsolationLevel());
          conn2.setAutoCommit(false);
          PreparedStatement pstmt2 = conn2
              .prepareStatement("update sellorders set cid = ? where oid = ?");
          pstmt2.setInt(1, 7);
          pstmt2.setInt(2, 3);
          assertEquals(1, pstmt2.executeUpdate());
          pstmt2.setInt(1, 3);
          pstmt2.setInt(2, 1);
          assertEquals(1, pstmt2.executeUpdate());

          // use a barrier to force txn1 to wait after first EX lock upgrade
          // and txn2 to wait before EX_SH lock acquisition
          getServerVM(1).invoke(TransactionDUnit.class, "installObservers");
          barrier.await();
          conn2.commit();
        } catch (Throwable t) {
          failure[0] = t;
        }
      }
    });
    t.start();

    pstmt = conn.prepareStatement("update sellorders "
        + "set status = 'cancelled' where order_time < ? and status = 'open'");
    pstmt.setTimestamp(1, now);
    barrier.await();
    try {
      pstmt.executeUpdate();
      fail("expected conflict exception");
    } catch (SQLException sqle) {
      if (!"X0Z02".equals(sqle.getSQLState())) {
        throw sqle;
      }
    }
    conn.close();

    t.join();

    if (failure[0] != null) {
      throw failure[0];
    }

    // clear the observers
    serverExecute(1, new SerializableRunnable() {
      @Override
      public void run() {
        GemFireCacheImpl.getExisting().getTxManager().setObserver(null);
        GemFireXDQueryObserverHolder.clearInstance();
      }
    });
  }

  public void _testBug41976() throws Exception {
    //startVMs(1, 1);
    Connection conn = getClientConnection(false);
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    Statement st = conn.createStatement();
    st.execute("create table customers (cid int not null, cust_name "
        + "varchar(100),  addr varchar(100), tid int, primary key (cid))" + getSuffix());
    PreparedStatement pstmt = conn
        .prepareStatement("insert into customers values(?,?,?,?)");
    pstmt.setInt(1, 1);
    pstmt.setString(2, "name1");
    pstmt.setString(3, "add1");
    pstmt.setInt(4, 1);
    pstmt.executeUpdate();
    conn.commit();

    // Now do an update on non existing row
    int n = st.executeUpdate("update customers set cust_name = 'dummy' "
        + "where cid=5");
    assertEquals(0, n);
    conn.commit();
    pstmt.setInt(1, 5);
    pstmt.setString(2, "name5");
    pstmt.setString(3, "add5");
    pstmt.setInt(4, 1);
    pstmt.executeUpdate();
    conn.commit();

    ResultSet rs = st.executeQuery("Select * from customers");
    int numRows = 0;
    while (rs.next()) {
      // Checking number of rows returned, since ordering of results
      // is not guaranteed.
      numRows++;
    }
    assertEquals("ResultSet should contain two rows ", 2, numRows);
    conn.commit();
    // Now do an update on non existing row
    n = st.executeUpdate("update  customers set cust_name = 'dummy' "
        + "where cid=6");
    assertEquals(0, n);

    pstmt.setInt(1, 6);
    pstmt.setString(2, "name6");
    pstmt.setString(3, "add6");
    pstmt.setInt(4, 1);
    pstmt.executeUpdate();
    conn.commit();
    rs = st.executeQuery("Select * from customers");
    numRows = 0;
    while (rs.next()) {
      // Checking number of rows returned, since ordering of results
      // is not guaranteed.
      numRows++;
    }
    assertEquals("ResultSet should contain two rows ", 3, numRows);
    conn.commit();
    conn.close();
  }

  public void _testBug41956() throws Exception {
    //startVMs(2, 2);
    Connection conn = getClientConnection(false);
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    Statement st = conn.createStatement();
    st.execute("create table trade.customers (cid int not null, cust_name "
        + "varchar(100), addr varchar(100), tid int, primary key (cid))  "
        + "partition by list (cid) (VALUES (16, 8, 10, 9), VALUES (7, 6, 0, 13),"
        + " VALUES (17, 5, 4, 15), VALUES (1, 2, 3, 11, 12, 14)) " + getSuffix());
    st.execute("create table trade.trades (tid int, cid int, eid int, "
        + "tradedate date, primary Key (tid), foreign key (cid)"
        + " references trade.customers (cid))  " + getSuffix());
    PreparedStatement pstmt = conn
        .prepareStatement("insert into trade.customers values(?,?,?,?)");
    pstmt.setInt(1, 1);
    pstmt.setString(2, "name1");
    pstmt.setString(3, "add1");
    pstmt.setInt(4, 1);
    pstmt.executeUpdate();
    conn.commit();

    int n = st.executeUpdate("update trade.customers set cust_name = 'dummy' "
        + "where cid = 1");
    assertEquals(1, n);
    this.gotConflict = false;
    Runnable runnable = new Runnable() {
      @Override
      public void run() {
        try {
          final Connection conn1 = getClientConnection(false);
          conn1.setAutoCommit(false);
          conn1.setTransactionIsolation(getIsolationLevel());
          Statement stmt = conn1.createStatement();
          try {
            assertEquals(1, stmt.executeUpdate("delete from trade.customers "
                + "where cid = 1 and tid = 1"));
          } catch (SQLException sqle) {
            if ("X0Z02".equals(sqle.getSQLState())) {
              gotConflict = true;
              conn1.rollback();
            } else {
              throw sqle;
            }
          }
        } catch (SQLException sqle) {
          gotConflict = false;
          getLogWriter().error("unexpected exception", sqle);
          fail("unexpected exception", sqle);
        }
      }
    };
    Thread th = new Thread(runnable);
    th.start();
    th.join();

    assertTrue("expected conflict", this.gotConflict);
    this.gotConflict = false;

    conn.commit();

    final ResultSet rs = st.executeQuery("select * from trade.customers");
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertEquals("dummy", rs.getString(2));
    assertEquals("add1", rs.getString(3));
    assertEquals(1, rs.getInt(1));
    assertFalse(rs.next());

    conn.commit();
    conn.close();
  }

  public void _testBug41974() throws Exception {
    //startVMs(1, 1);
    Connection conn = getClientConnection(false);
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    Statement st = conn.createStatement();
    st.execute("create table customers (cid int not null, cust_name "
        + "varchar(100),  addr varchar(100), tid int, primary key (cid))" + getSuffix());
    PreparedStatement pstmt = conn
        .prepareStatement("insert into customers values(?,?,?,?)");
    pstmt.setInt(1, 1);
    pstmt.setString(2, "name1");
    pstmt.setString(3, "add1");
    pstmt.setInt(4, 1);
    assertEquals(1, pstmt.executeUpdate());
    conn.commit();
    // Now do an update on non existing row
    assertEquals(0, st.executeUpdate("update customers set "
        + "cust_name = 'dummy' where cid=5"));
    conn.commit();
    pstmt.setInt(1, 5);
    pstmt.setString(2, "name5");
    pstmt.setString(3, "add5");
    pstmt.setInt(4, 1);
    assertEquals(1, pstmt.executeUpdate());
    conn.commit();
    assertEquals(1, st.executeUpdate("update  customers set "
        + "cust_name = 'dummy' where cid=5"));
    conn.commit();
    conn.close();
  }

  public void _testBug42014() throws Exception {
    // Create a table from client using partition by column
    // Start one client and three servers
    //startVMs(1, 3);

    clientSQLExecute(1, "create table trade.portfolio (cid int not null, "
        + "sid int not null, qty int not null, availQty int not null, "
        + "subTotal decimal(30,20), tid int) "
        + "partition by list (tid) (values(1,2))" + getSuffix());
    Connection conn = getClientConnection(false);
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    clientSQLExecute(1,
        "Insert into  trade.portfolio values(1,1,1724, 1,7865.66666,1)");
    clientSQLExecute(1,
        "Insert into  trade.portfolio values(2,1,1731, 1,7865.66666,1)");

    conn.commit();
    PreparedStatement pstmt = conn
        .prepareStatement("update trade.portfolio set "
            + "subTotal = qty * ? where sid = ? ");
    pstmt.setBigDecimal(1, new BigDecimal(13.96));
    pstmt.setInt(2, 1);
    assertEquals(2, pstmt.executeUpdate());
    conn.commit();
    // String query = "select type, id, name from t1 where id IN (1,?,2, ?)";
    String query = "select cid, subTotal from trade.portfolio where  sid = 1 ";
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery(query);
    assertTrue(rs.next());
    BigDecimal value = rs.getBigDecimal(2);
    if (rs.getInt(1) == 1) {
      assertEquals(22412, value.intValue());
    } else {
      assertEquals(22503, value.intValue());
    }
    assertTrue(rs.next());
    value = rs.getBigDecimal(2);
    if (rs.getInt(1) == 1) {
      assertEquals(22412, value.intValue());
    } else {
      assertEquals(22503, value.intValue());
    }
    conn.commit();
  }

  public void _testBug42031IsolationAndTXData() throws Exception {
    // Create the controller VM as client which belongs to default server group
    startClientVMs(1, 0, null);
    startServerVMs(1, -1, "SG1");
    // create table
    clientSQLExecute(1, "create table TESTTABLE (ID int not null primary key, "
        + "DESCRIPTION varchar(1024), ADDRESS varchar(1024), ID1 int)" + getSuffix());

    Connection conn = getClientConnection(false);
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    Statement stmt = conn.createStatement();
    // Do an insert in sql fabric. This will create a primary bucket on the lone
    // server VM
    // with bucket ID =1
    stmt.executeUpdate("Insert into TESTTABLE values(114,'desc114','Add114',114)");

    stmt.executeUpdate("Insert into TESTTABLE values(1,'desc1','Add1',1)");
    stmt.executeUpdate("Insert into TESTTABLE values(227,'desc227','Add227',227)");
    stmt.executeUpdate("Insert into TESTTABLE values(340,'desc340','Add340',340)");
    conn.rollback();
    stmt.executeUpdate("Insert into TESTTABLE values(114,'desc114','Add114',114)");
    stmt.executeUpdate("Insert into TESTTABLE values(2,'desc1','Add1',1)");
    stmt.executeUpdate("Insert into TESTTABLE values(224,'desc227','Add227',227)");
    stmt.executeUpdate("Insert into TESTTABLE values(331,'desc340','Add340',340)");
    conn.commit();
    // Bulk Update
    stmt.executeUpdate("update TESTTABLE set ID1 = ID1 +1 ");
    ResultSet rs = stmt.executeQuery("select ID1 from  TESTTABLE");
    Set<Integer> expected = new HashSet<Integer>();
    expected.add(Integer.valueOf(1));
    expected.add(Integer.valueOf(227));
    expected.add(Integer.valueOf(340));
    expected.add(Integer.valueOf(114));
    Set<Integer> expected2 = new HashSet<Integer>();
    expected2.add(Integer.valueOf(2));
    expected2.add(Integer.valueOf(228));
    expected2.add(Integer.valueOf(341));
    expected2.add(Integer.valueOf(115));

    int numRows = 0;
    while (rs.next()) {
      Integer got = Integer.valueOf(rs.getInt(1));
      assertTrue(expected2.contains(got));
      ++numRows;
    }
    assertEquals(expected2.size(), numRows);

    // rollback and check original values
    conn.rollback();

    rs = stmt.executeQuery("select ID1 from TESTTABLE");
    numRows = 0;
    while (rs.next()) {
      Integer got = Integer.valueOf(rs.getInt(1));
      assertTrue(expected.contains(got));
      ++numRows;
    }
    assertEquals(expected.size(), numRows);

    // now commit and check success
    stmt.executeUpdate("update TESTTABLE set ID1 = ID1 +1 ");
    rs = stmt.executeQuery("select ID1 from TESTTABLE");
    numRows = 0;
    while (rs.next()) {
      Integer got = Integer.valueOf(rs.getInt(1));
      assertTrue(expected2.contains(got));
      ++numRows;
    }
    assertEquals(expected2.size(), numRows);

    conn.commit();

    rs = stmt.executeQuery("select ID1 from TESTTABLE");
    numRows = 0;
    while (rs.next()) {
      Integer got = Integer.valueOf(rs.getInt(1));
      assertTrue(expected2.contains(got));
      ++numRows;
    }
    assertEquals(expected2.size(), numRows);
  }

  public void _testBug41873_1() throws Exception {

    // Create the controller VM as client which belongs to default server group
    startClientVMs(1, 0, null);
    startServerVMs(2, -1, "SG1");
    Connection conn = getClientConnection(false);
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    // create table
    clientSQLExecute(1, "Create table t1 (c1 int not null , c2 int not null, "
        + "c3 int not null, c4 int not null) redundancy 1 "
        + "partition by column (c1) " + getSuffix());
    conn.commit();
    Statement st = conn.createStatement();
    st.execute("insert into t1 values (1, 1,1,1)");
    st.execute("insert into t1 values (114, 114,114,114)");
    conn.commit();
    st.execute("update t1 set c2 =2 where c1 =1");
    st.execute("update t1 set c3 =3 where c1 =1");
    st.execute("update t1 set c4 =4 where c1 =1");
    st.execute("update t1 set c2 =3 where c1 = 114");
    st.execute("update t1 set c3 =4 where c1 =114");
    st.execute("update t1 set c4 =5 where c1 =114");
    conn.commit();
    ResultSet rs = st.executeQuery("Select * from t1 where c1 = 1");
    rs.next();
    assertEquals(1, rs.getInt(1));
    assertEquals(2, rs.getInt(2));
    assertEquals(3, rs.getInt(3));
    assertEquals(4, rs.getInt(4));

    rs = st.executeQuery("Select * from t1 where c1 = 114");
    rs.next();
    assertEquals(114, rs.getInt(1));
    assertEquals(3, rs.getInt(2));
    assertEquals(4, rs.getInt(3));
    assertEquals(5, rs.getInt(4));
    conn.commit();
  }

  public void _testBug42067_1() throws Exception {

    // Create the controller VM as client which belongs to default server group
    startClientVMs(1, 0, null);
    startServerVMs(2, -1, "SG1");
    Connection conn = getClientConnection(false);
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    // create table
    clientSQLExecute(1, "Create table t1 (c1 int not null, "
        + "c2 int not null, c3 int not null, c4 int not null) "
        + "redundancy 1 partition by column (c1) " + getSuffix());
    conn.commit();
    Statement st = conn.createStatement();
    st.execute("insert into t1 values (1, 1,1,1)");
    st.execute("insert into t1 values (114, 114,114,114)");
    conn.commit();
    st.execute("delete from t1 where c1 =1 and c3 =1");
    st.execute("update t1 set c2 =2 where c1 =1 and c3 =1");
    conn.commit();
  }

  public void _testBug42067_2() throws Exception {

    // Create the controller VM as client which belongs to default server group
    startClientVMs(1, 0, null);
    startServerVMs(2, -1, "SG1");
    Connection conn = getClientConnection(false);
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    // create table
    clientSQLExecute(1, "Create table t1 (c1 int not null primary key, "
        + "c2 int not null, c3 int not null, c4 int not null) "
        + "redundancy 1 partition by column (c1) " + getSuffix());
    conn.commit();
    Statement st = conn.createStatement();
    st.execute("insert into t1 values (1, 1,1,1)");
    st.execute("insert into t1 values (114, 114,114,114)");
    conn.commit();
    st.execute("delete from t1 where c1 =1 and c3 =1");
    st.execute("update t1 set c2 =2 where c1 =1 and c3 =1");
    conn.commit();
    ResultSet rs = st.executeQuery("select * from t1");
    assertTrue(rs.next());
    assertEquals(114, rs.getInt(1));
    assertFalse(rs.next());
  }

  public void _testBug42311_1() throws Exception {
    //startVMs(1, 1);
    Connection conn = getClientConnection(false);
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    // create table
    clientSQLExecute(1, "Create table t1 (c1 int not null primary key, "
        + "c2 int not null, c3 int not null, c4 int not null) "
        + "partition by column (c2) " + getSuffix());
    conn.commit();
    Statement st = conn.createStatement();
    st.execute("insert into t1 values (4, 20,13,7), (5, 30, 56,7), "
        + "(3, 40,7,7), (6, 30,7,7), (8, 5,6,6)");

    conn.commit();
  }

  public void _testBug42311_2() throws Exception {
    //startVMs(1, 1);
    Connection conn = getClientConnection(false);
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    // create table
    clientSQLExecute(1, "Create table t1 (c1 int not null primary key, "
        + "c2 int not null, c3 int not null, c4 int not null) "
        + "partition by column (c2) " + getSuffix());
    conn.commit();
    PreparedStatement ps = conn
        .prepareStatement("insert into t1 values (?,?,?,?)");
    for (int i = 0; i < 10; i++) {

      ps.setInt(1, i);
      ps.setInt(2, i);
      ps.setInt(3, i);
      ps.setInt(4, i);
      ps.addBatch();
    }
    ps.executeBatch();
    conn.commit();
  }

  public void _testBugPutAllDataLossAsBuggyDistribution() throws Exception {
    reduceLogLevelForTest("config");
    startServerVMs(2, 0, null);
    final int netPort = startNetworkServer(1, null, null);
    final Connection conn = TestUtil.getNetConnection(netPort, null, null);

    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    // create table
    serverSQLExecute(1, "Create table app.t1 (c1 int not null primary key, "
        + "c2 int not null, c3 int not null, c4 int not null) "
        + "partition by column (c2) redundancy 1" + getSuffix());
    conn.commit();
    getLogWriter().info("created table app.t1");
    Statement s = conn.createStatement();
    s.execute("insert into app.t1 values(10, 10, 10, 10)");
    PreparedStatement ps = conn
        .prepareStatement("insert into app.t1 values (?,?,?,?)");
    for (int i = 0; i < 10; i++) {
      ps.setInt(1, i);
      ps.setInt(2, 2);
      ps.setInt(3, i);
      ps.setInt(4, i);

      ps.addBatch();
    }
    ps.executeBatch();
    getLogWriter().info("commiting batch");
    conn.commit();
    TXManagerImpl.waitForPendingCommitForTest();
    getLogWriter().info("verifying");
    for (VM vm : serverVMs) {
      vm.invoke(TransactionDUnit.class, "verifyNumEntries", new Object[]{
          Integer.valueOf(1), Integer.valueOf(10), "APP/T1"});
    }
    for (VM vm : serverVMs) {
      vm.invoke(TransactionDUnit.class, "verifyNumEntries", new Object[]{
          Integer.valueOf(10), Integer.valueOf(2), "APP/T1"});
    }
  }

  public static void verifyNumEntries(int expectedNum, int bucketid,
      String regionName) {
    getGlobalLogger().info("executing verifyNumEntries");
    Region<?, ?> r = Misc.getRegion(regionName, true, false);
    PartitionedRegion pr = (PartitionedRegion)r;
    PartitionedRegionDataStore ds = pr.getDataStore();
    Set<BucketRegion> brs = ds.getAllLocalBucketRegions();
    for (BucketRegion b : brs) {
      if (b.getId() != bucketid) {
        continue;
      }
      int entryCnt = b.entryCount();
      getGlobalLogger().info(
          "executing verifyNumEntries for bucket region: " + b
              + " entry count is: " + entryCnt);
      assertEquals(expectedNum, entryCnt);
    }
  }

  public void _testMultipleInsertFromThinClient_bug44242() throws Exception {
    startServerVMs(2, 0, null);
    int port = startNetworkServer(1, null, null);
    Connection netConn = TestUtil.getNetConnection(port, null, null);
    netConn.createStatement().execute("create schema emp");
    netConn.close();

    for (int i = 0; i < 2; i++) {
      Connection netConn1 = TestUtil.getNetConnection(port, null, null);

      Connection netConn2 = TestUtil.getNetConnection(port, null, null);

      Statement s = netConn1.createStatement();
      String ext = "";
      if (i == 1) {
        ext = "replicate";
      }
      s.execute("create table emp.EMPLOYEE_parent(lastname varchar(30) "
          + "primary key, depId int)" + ext + getSuffix());
      s.execute("create table emp.EMPLOYEE(lastname varchar(30) primary key, "
          + "depId int, foreign key(lastname) references "
          + "emp.EMPLOYEE_parent(lastname) on delete restrict)" + ext + getSuffix());
      s.execute("insert into emp.EMPLOYEE_parent values('Jones', 10), "
          + "('Rafferty', 50), ('Robinson', 100)");

      netConn2.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      netConn2.setAutoCommit(false);
      Statement s2 = netConn2.createStatement();
      s2.execute("delete from emp.EMPLOYEE_parent");
      s2.execute("select * from emp.employee_parent");
      netConn1.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      netConn1.setAutoCommit(false);
      PreparedStatement pstmnt = netConn1
          .prepareStatement("INSERT INTO emp.employee VALUES (?, ?)");

      pstmnt.setString(1, "Jones");
      pstmnt.setInt(2, 33);
      pstmnt.addBatch();

      pstmnt.setString(1, "Rafferty");
      pstmnt.setInt(2, 31);
      pstmnt.addBatch();

      pstmnt.setString(1, "Robinson");
      pstmnt.setInt(2, 34);
      pstmnt.addBatch();

      try {
        pstmnt.executeBatch();
        netConn1.commit();
        fail("commit should have failed");
      } catch (SQLException e) {
        if (!"X0Z02".equals(e.getSQLState())) {
          throw e;
        }
      }
      netConn2.commit();

      s.execute("drop table emp.employee");
      s.execute("drop table emp.employee_parent");
    }
  }

  // FK related tests
  public void _testFK_NoGlobalIndex_differentThread() throws Exception {
    //startVMs(1, 2);
    Connection conn = getClientConnection(false);
    clientSQLExecute(1, "Create table t1 (c1 int not null primary key, "
        + "c2 int not null, c3 int not null)" + getSuffix());
    clientSQLExecute(1, "Create table t2 (c1 int not null primary key, "
        + "c2 int not null, c3 int not null, "
        + "foreign key (c1) references t1(c1))" + getSuffix());
    clientSQLExecute(1, "insert into t1 values(1, 1, 1)");
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    Statement stmt = conn.createStatement();
    stmt.execute("insert into t2 values(1, 1, 1)");
    try {
      stmt.execute("insert into t2 values(2, 2, 2)");
      fail("the above insert should have failed");
    } catch (SQLException ex) {
      TestUtil.getLogger().info("exception state is: " + ex.getSQLState(), ex);
      if (!"23503".equals(ex.getSQLState())) {
        throw ex;
      }
    }
    stmt.execute("insert into t1 values(2, 2, 2)");
    stmt.execute("insert into t2 values(2, 2, 2)");

    this.threadEx = null;
    Thread th = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Connection newConn = getClientConnection(false);
          newConn
              .setTransactionIsolation(getIsolationLevel());
          newConn.setAutoCommit(false);
          Statement newstmt = newConn.createStatement();
          try {
            addExpectedException(new int[]{1}, new int[]{1},
                ConflictException.class);
            newstmt.execute("insert into t2 values(2, 2, 2)");
            fail("ConflictException should have come");
          } catch (SQLException sqle) {
            if (!"X0Z02".equals(sqle.getSQLState())) {
              throw sqle;
            }
          }
          removeExpectedException(new int[]{}, new int[]{},
              ConflictException.class);
          try {
            newstmt.execute("insert into t2 values(3, 3, 3)");
            fail("the above insert should have failed");
          } catch (SQLException ex) {
            TestUtil.getLogger().info(
                "exception state is: " + ex.getSQLState(), ex);
            if (!"23503".equals(ex.getSQLState())) {
              throw ex;
            }
            newstmt.close();
            newConn.commit();
          }
        } catch (Throwable t) {
          getLogWriter().error("unexpected exception", t);
          threadEx = t;
          fail("unexpected exception " + t);
        }
      }
    });

    th.start();
    th.join();
    if (this.threadEx != null) {
      throw new TestException("unexpected exception in thread", this.threadEx);
    }
    stmt.close();
    conn.commit();
  }

  public void _testFK_NoGlobalIndexSameThread() throws Exception {
    //startVMs(1, 2);
    Connection conn = getClientConnection(false);
    clientSQLExecute(1, "Create table t1 (c1 int not null primary key, "
        + "c2 int not null, c3 int not null)" + getSuffix());
    clientSQLExecute(1, "Create table t2 (c1 int not null primary key, "
        + "c2 int not null, c3 int not null, "
        + "foreign key (c1) references t1(c1))" + getSuffix());
    clientSQLExecute(1, "insert into t1 values(1, 1, 1)");
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    Statement stmt = conn.createStatement();
    stmt.execute("insert into t2 values(1, 1, 1)");
    try {
      stmt.execute("insert into t2 values(2, 2, 2)");
      fail("the above insert should have failed");
    } catch (SQLException ex) {
      TestUtil.getLogger().info("exception state is: " + ex.getSQLState(), ex);
      if (!"23503".equals(ex.getSQLState())) {
        throw ex;
      }
    }
    stmt.execute("insert into t1 values(2, 2, 2)");
    stmt.execute("insert into t2 values(2, 2, 2)");

    Connection newConn = getClientConnection(false);
    newConn.setTransactionIsolation(getIsolationLevel());
    newConn.setAutoCommit(false);
    Statement newstmt = newConn.createStatement();
    try {
      addExpectedException(new int[]{1}, new int[]{1},
          ConflictException.class);
      newstmt.execute("insert into t2 values(2, 2, 2)");
      fail("ConflictException should have come");
    } catch (SQLException sqle) {
      if (!"X0Z02".equals(sqle.getSQLState())) {
        throw sqle;
      }
    }
    removeExpectedException(new int[]{}, new int[]{}, ConflictException.class);
    try {
      newstmt.execute("insert into t2 values(3, 3, 3)");
      fail("the above insert should have failed");
    } catch (SQLException ex) {
      TestUtil.getLogger().info("exception state is: " + ex.getSQLState(), ex);
      if (!"23503".equals(ex.getSQLState())) {
        throw ex;
      }
    }
    stmt.close();
    conn.commit();
    newstmt.close();
    newConn.commit();
  }

  public void _testFK_GlobalIndexDifferentThread() throws Exception {
    //startVMs(1, 2);
    Connection conn = getClientConnection(false);
    clientSQLExecute(1, "Create table t1 (c1 int not null primary key, "
        + "c2 int not null, c3 int not null) partition by column(c2)" + getSuffix());
    clientSQLExecute(1, "Create table t2 (c1 int not null primary key, "
        + "c2 int not null, c3 int not null, "
        + "foreign key (c1) references t1(c1))" + getSuffix());
    clientSQLExecute(1, "insert into t1 values(1, 1, 1)");
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    Statement stmt = conn.createStatement();
    stmt.execute("insert into t2 values(1, 1, 1)");
    try {
      stmt.execute("insert into t2 values(2, 2, 2)");
      fail("the above insert should have failed");
    } catch (SQLException ex) {
      TestUtil.getLogger().info("exception state is: " + ex.getSQLState(), ex);
      if (!"23503".equals(ex.getSQLState())) {
        throw ex;
      }
    }
    stmt.execute("insert into t1 values(2, 2, 2)");
    stmt.execute("insert into t2 values(2, 2, 2)");

    this.threadEx = null;
    Thread th = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Connection newConn = getClientConnection(false);
          newConn
              .setTransactionIsolation(getIsolationLevel());
          newConn.setAutoCommit(false);
          Statement newstmt = newConn.createStatement();
          try {
            addExpectedException(new int[]{1}, new int[]{1},
                ConflictException.class);
            newstmt.execute("insert into t2 values(2, 2, 2)");
            fail("ConflictException should have come");
          } catch (SQLException sqle) {
            if (!"X0Z02".equals(sqle.getSQLState())) {
              throw sqle;
            }
          }
          removeExpectedException(new int[]{}, new int[]{},
              ConflictException.class);
          try {
            newstmt.execute("insert into t2 values(3, 3, 3)");
            fail("the above insert should have failed");
          } catch (SQLException ex) {
            TestUtil.getLogger().info(
                "exception state is: " + ex.getSQLState(), ex);
            if (!"23503".equals(ex.getSQLState())) {
              throw ex;
            }
            newstmt.close();
            newConn.commit();
          }
        } catch (Throwable t) {
          getLogWriter().error("unexpected exception", t);
          threadEx = t;
        }
      }
    });

    th.start();
    th.join();
    if (this.threadEx != null) {
      throw new TestException("unexpected exception in thread", this.threadEx);
    }

    stmt.close();
    conn.commit();
  }

  public void _testFK_GlobalIndexSameThread() throws Exception {
    //startVMs(1, 2);
    Connection conn = getClientConnection(false);
    clientSQLExecute(1, "Create table t1 (c1 int not null primary key, "
        + "c2 int not null, c3 int not null) partition by column(c2)" + getSuffix());
    clientSQLExecute(1, "Create table t2 (c1 int not null primary key, "
        + "c2 int not null, c3 int not null, "
        + "foreign key (c1) references t1(c1))" + getSuffix());
    clientSQLExecute(1, "insert into t1 values(1, 1, 1)");
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    Statement stmt = conn.createStatement();
    stmt.execute("insert into t2 values(1, 1, 1)");
    try {
      stmt.execute("insert into t2 values(2, 2, 2)");
      fail("the above insert should have failed");
    } catch (SQLException ex) {
      TestUtil.getLogger().info("exception state is: " + ex.getSQLState(), ex);
      if (!"23503".equals(ex.getSQLState())) {
        throw ex;
      }
    }
    stmt.execute("insert into t1 values(2, 2, 2)");
    stmt.execute("insert into t2 values(2, 2, 2)");

    Connection newConn = getClientConnection(false);
    newConn.setTransactionIsolation(getIsolationLevel());
    newConn.setAutoCommit(false);
    Statement newstmt = newConn.createStatement();
    try {
      addExpectedException(new int[]{1}, new int[]{1},
          ConflictException.class);
      newstmt.execute("insert into t2 values(2, 2, 2)");
      fail("ConflictException should have come");
    } catch (SQLException sqle) {
      if (!"X0Z02".equals(sqle.getSQLState())) {
        throw sqle;
      }
    }
    removeExpectedException(new int[]{}, new int[]{}, ConflictException.class);
    try {
      newstmt.execute("insert into t2 values(3, 3, 3)");
      fail("the above insert should have failed");
    } catch (SQLException ex) {
      TestUtil.getLogger().info("exception state is: " + ex.getSQLState(), ex);
      if (!"23503".equals(ex.getSQLState())) {
        throw ex;
      }
    }
    stmt.close();
    conn.commit();
    newstmt.close();
    newConn.commit();
  }

  public void _testFKWithBatching_49371() throws Throwable {
    //startVMs(1, 2);

    Connection conn = getClientConnection(false);
    Statement stmt = conn.createStatement();

    // Ensure that containsKey and referenceKey checks are on separate
    // servers. Three scenarios:
    // a) parent insert, child insert starts after that, then it should fail
    // b) child insert starts after parent delete and commits (child should fail
    //    with conflict)
    // c) parent delete starts after child transactional insert but before its
    //    commit, then both commit (one or both should fail)
    stmt.execute("create table parent (pid int primary key, "
        + "tid int unique) replicate");
    stmt.execute("create table child (cid int primary key, pid int, "
        + "constraint p_fk foreign key (pid) references parent (pid) "
        + "on delete restrict) replicate");
    stmt.execute("create table child2 (cid int primary key, tid int, "
        + "constraint t_fk foreign key (tid) references parent (tid) "
        + "on delete restrict) replicate");

    AsyncInvocation asyncServer = getServerVM(1).invokeAsync(
        new SerializableRunnable() {

          @Override
          public void run() {
            try {
              final AnyCyclicBarrier barrier = new AnyCyclicBarrier(
                  2, "fk_barrier");
              final long waitMillis = 10000L;
              Connection conn = getClientConnection(false);
              conn.setTransactionIsolation(getIsolationLevel());
              conn.setAutoCommit(false);
              Statement stmt = conn.createStatement();

              stmt.execute("insert into parent values (1, 2)");
              stmt.execute("insert into parent values (2, 3)");
              // wait for child to start its insert
              barrier.await(waitMillis);
              // wait for child to start complete (and fail) insert
              barrier.await(waitMillis);
              conn.rollback();

              stmt.execute("insert into parent values (1, 2), (2, 3)");
              conn.commit();

              stmt.execute("delete from parent where pid=1");
              stmt.execute("delete from parent where pid=2");
              // wait for child to do its insert
              barrier.await(waitMillis);
              // now wait for child to complete (and fail) its insert
              barrier.await(waitMillis);
              conn.commit();

              stmt.execute("insert into parent values (3, 4), (4, 5)");
              conn.commit();

              // signal the child to start its insert
              barrier.await(waitMillis);
              // wait for child to do its insert
              barrier.await(waitMillis);
              try {
                stmt.execute("delete from parent where pid=3");
                conn.commit();
                fail("expected to fail with conflict");
              } catch (SQLException sqle) {
                if (!"X0Z02".equals(sqle.getSQLState())) {
                  throw sqle;
                }
              }
              // do the same for child with unique FK constraint
              barrier.await(waitMillis);
              barrier.await(waitMillis);
              try {
                stmt.execute("delete from parent where pid=4");
                conn.commit();
                fail("expected to fail with conflict");
              } catch (SQLException sqle) {
                if (!"X0Z02".equals(sqle.getSQLState())) {
                  throw sqle;
                }
              }
              barrier.await(waitMillis);
            } catch (Throwable t) {
              getLogWriter().error(t);
              throw new TestException("unexpected exception", t);
            }
          }
        });

    AsyncInvocation asyncChild = getServerVM(2).invokeAsync(
        new SerializableRunnable() {

          @Override
          public void run() {
            try {
              final AnyCyclicBarrier barrier = new AnyCyclicBarrier(
                  2, "fk_barrier");
              final long waitMillis = 10000L;
              Connection conn = getClientConnection(false);
              conn.setTransactionIsolation(getIsolationLevel());
              conn.setAutoCommit(false);
              Statement stmt = conn.createStatement();

              // wait for parent insert to begin
              barrier.await(waitMillis);
              try {
                stmt.execute("insert into child values (1, 1)");
                fail("expected to fail with constraint violation");
              } catch (SQLException sqle) {
                if (!"23503".equals(sqle.getSQLState())) {
                  throw sqle;
                }
              }
              try {
                stmt.execute("insert into child values (2, 2)");
                fail("expected to fail with constraint violation");
              } catch (SQLException sqle) {
                if (!"23503".equals(sqle.getSQLState())) {
                  throw sqle;
                }
              }
              try {
                stmt.execute("insert into child2 values (2, 2)");
                fail("expected to fail with constraint violation");
              } catch (SQLException sqle) {
                if (!"23503".equals(sqle.getSQLState())) {
                  throw sqle;
                }
              }
              try {
                stmt.execute("insert into child2 values (3, 3)");
                fail("expected to fail with constraint violation");
              } catch (SQLException sqle) {
                if (!"23503".equals(sqle.getSQLState())) {
                  throw sqle;
                }
              }
              // signal parent to proceed
              barrier.await(waitMillis);

              // wait for parent delete to begin
              barrier.await(waitMillis);
              try {
                stmt.execute("insert into child values (1, 1)");
                conn.commit();
                fail("expected to fail with conflict");
              } catch (SQLException sqle) {
                if (!"X0Z02".equals(sqle.getSQLState())) {
                  throw sqle;
                }
              }
              try {
                stmt.execute("insert into child values (2, 2)");
                conn.commit();
                fail("expected to fail with conflict");
              } catch (SQLException sqle) {
                if (!"X0Z02".equals(sqle.getSQLState())) {
                  throw sqle;
                }
              }
              try {
                stmt.execute("insert into child2 values (2, 2)");
                conn.commit();
                fail("expected to fail with conflict");
              } catch (SQLException sqle) {
                if (!"X0Z02".equals(sqle.getSQLState())) {
                  throw sqle;
                }
              }
              try {
                stmt.execute("insert into child2 values (3, 3)");
                conn.commit();
                fail("expected to fail with conflict");
              } catch (SQLException sqle) {
                if (!"X0Z02".equals(sqle.getSQLState())) {
                  throw sqle;
                }
              }
              // allow parent to proceed with its commit
              barrier.await(waitMillis);

              // wait for parent to complete its insert
              barrier.await(waitMillis);
              // now start with child insert first
              stmt.execute("insert into child values (3, 3)");
              // signal parent
              barrier.await(waitMillis);
              // wait for parent to fail
              barrier.await(waitMillis);
              conn.commit();

              // now the same for child with unique FK constraint
              stmt.execute("insert into child2 values (5, 5)");
              barrier.await(waitMillis);
              barrier.await(waitMillis);
              conn.commit();
            } catch (Throwable t) {
              getLogWriter().error(t);
              throw new TestException("unexpected exception", t);
            }
          }
        });

    asyncServer.join();
    asyncChild.join();

    AnyCyclicBarrier.destroy("fk_barrier");

    Throwable failure = null;
    if (asyncServer.exceptionOccurred()) {
      failure = asyncServer.getException();
    }
    if (asyncChild.exceptionOccurred()) {
      if (failure != null) {
        Throwable cause = failure;
        while (cause.getCause() != null) {
          cause = cause.getCause();
        }
        cause.initCause(asyncChild.getException());
      } else {
        failure = asyncChild.getException();
      }
    }
    if (failure != null) {
      throw failure;
    }

    // check final expected values in parent and child tables
    Object[][] parentValues = new Object[][]{new Object[]{4, 5},
        new Object[]{3, 4}};
    Object[][] childValues = new Object[][]{new Object[]{3, 3}};
    Object[][] child2Values = new Object[][]{new Object[]{5, 5}};

    ResultSet rs = stmt.executeQuery("select * from parent");
    JDBC.assertUnorderedResultSet(rs, parentValues, false);

    rs = stmt.executeQuery("select * from child");
    JDBC.assertUnorderedResultSet(rs, childValues, false);

    rs = stmt.executeQuery("select * from child2");
    JDBC.assertUnorderedResultSet(rs, child2Values, false);
  }

  /**
   * Test for bug #42822 that fails when reading TX deleted rows (due to deletes
   * or updates) in another TX.
   */
  public void _test42822() throws Exception {
    //startVMs(1, 3);

    Connection conn = getClientConnection(false);
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    Statement stmt = conn.createStatement();
    stmt.execute("create table oorder ("
        + "o_w_id       integer      not null,"
        + "o_d_id       integer      not null,"
        + "o_id         integer      not null,"
        + "o_c_id       integer,"
        + "o_carrier_id integer,"
        + "o_ol_cnt     decimal(2,0),"
        + "o_all_local  decimal(1,0),"
        + "o_entry_d    timestamp"
        + ") partition by column (o_w_id)" + getSuffix());
    stmt.execute("create index oorder_carrier1 on oorder (o_w_id)");
    stmt.execute("create index oorder_carrier2 on oorder (o_d_id)");
    stmt.execute("create index oorder_carrier3 on oorder (o_c_id)");
    // some inserts, deletes and updates
    stmt.execute("insert into oorder (o_w_id, o_d_id, o_id, o_c_id) values "
        + "(1, 2, 2, 1), (2, 4, 4, 2), (3, 6, 6, 3), (4, 8, 8, 4)");
    conn.commit();
    stmt.execute("delete from oorder where o_d_id > 6");
    stmt.execute("update oorder set o_c_id = o_w_id + 2 where o_w_id < 2");
    stmt.execute("update oorder set o_c_id = o_id + 1 where o_w_id >= 3");
    // now a select with open transaction using another connection
    this.threadEx = null;
    final CyclicBarrier barrier = new CyclicBarrier(2);
    Thread t = new Thread(new Runnable() {

      @Override
      public void run() {
        try {
          Connection conn = getClientConnection(false);
          conn.setTransactionIsolation(getIsolationLevel());
          conn.setAutoCommit(false);
          checkResultsFor42822(conn, false);
          // wait for other thread to sync up
          barrier.await();
          // wait for other thread to complete commit
          barrier.await();
          checkResultsFor42822(conn, true);
          conn.commit();
        } catch (Throwable t) {
          getLogWriter().error("unexpected exception", t);
          threadEx = t;
        }
      }
    });
    t.start();
    checkResultsFor42822(conn, true);
    barrier.await();
    if (this.threadEx != null) {
      throw new TestException("unexpected exception in thread", this.threadEx);
    }
    conn.commit();
    // now check for updated results in this thread and other
    checkResultsFor42822(conn, true);
    barrier.await();
    t.join();
    if (this.threadEx != null) {
      throw new TestException("unexpected exception in thread", this.threadEx);
    }
  }

  /**
   * Test for "sync-commits" property.
   */
  public void _testSyncCommits() throws Throwable {
    //startVMs(1, 3);

    final int netPort = startNetworkServer(2, null, null);

    Properties props = new Properties();
    props.setProperty("sync-commits", "true");
    Connection conn = TestUtil.getConnection(props);
    Connection netConn = TestUtil.getNetConnection(netPort, null, props);
    Statement st = conn.createStatement();
    Statement netSt = netConn.createStatement();
    st.execute("create schema tran");
    st.execute("Create table tran.t1 (c1 int not null , c2 int not null, "
        + "primary key(c1)) partition by primary key redundancy 2" + getSuffix());
    conn.commit();
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    st = conn.createStatement();
    netConn.setTransactionIsolation(getIsolationLevel());
    netConn.setAutoCommit(false);

    final int numThreads = 5;
    final int numItems = 10;
    final Thread[] thrs = new Thread[numThreads];
    final CyclicBarrier barrier = new CyclicBarrier(numThreads + 1);
    final Throwable[] failure = new Throwable[1];
    for (int i = 0; i < numThreads; i++) {
      final int thrNum = i;
      thrs[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            Connection conn;
            if ((thrNum % 2) == 0) {
              conn = getClientConnection(false);
            } else {
              conn = TestUtil.getNetConnection(netPort, null, null);
            }
            PreparedStatement pstmt = conn
                .prepareStatement("select count(*) from tran.t1");
            barrier.await();
            ResultSet rs = pstmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(numItems, rs.getInt(1));
            assertFalse(rs.next());
            pstmt.close();
            conn.close();
            barrier.await(10, TimeUnit.SECONDS);
          } catch (Throwable t) {
            failure[0] = t;
          }
        }
      });
      thrs[i].start();
    }

    for (int i = 1; i <= numItems; i++) {
      String ins = "insert into tran.t1 values (" + (i * 10) + ", " + (i * 10)
          + ")";
      if ((i % 2) == 0) {
        st.execute(ins);
      } else {
        netSt.execute(ins);
      }
    }

    // install a transaction observer on one of the servers to force failure
    // with async commits
    serverExecute(1, new SerializableRunnable() {
      @Override
      public void run() {
        TXManagerImpl txMgr = GemFireCacheImpl.getExisting()
            .getCacheTransactionManager();
        txMgr.setObserver(new TransactionObserverAdapter() {
          @Override
          public void afterIndividualCommitPhase1(TXStateProxy tx,
              Object callbackArg) {
            try {
              Thread.sleep(3000);
            } catch (InterruptedException ie) {
              // ignore
            }
          }
        });
      }
    });

    conn.commit();
    netConn.commit();
    barrier.await();
    if (failure[0] != null) {
      fail("unexpected failure in thread", failure[0]);
    }
    try {
      barrier.await(10, TimeUnit.SECONDS);
    } catch (Exception e) {
      if (failure[0] != null) {
        fail("unexpected failure in thread for " + e, failure[0]);
      } else {
        throw e;
      }
    }
    ResultSet rs = st.executeQuery("Select * from tran.t1");
    int numRows = 0;
    while (rs.next()) {
      // Checking number of rows returned, since ordering of results
      // is not guaranteed. We can write an order by query for this (another
      // test).
      numRows++;
    }
    assertEquals("ResultSet should contain " + numItems + " rows ", numItems,
        numRows);
    VM vm = this.serverVMs.get(0);
    vm.invoke(getClass(), "checkData",
        new Object[]{"TRAN.T1", Long.valueOf(numItems)});

    serverExecute(1, new SerializableRunnable() {
      @Override
      public void run() {
        TXManagerImpl txMgr = GemFireCacheImpl.getExisting()
            .getCacheTransactionManager();
        txMgr.setObserver(null);
      }
    });

    // Close connection, resultset etc...
    rs.close();
    st.close();
    conn.commit();
    conn.close();
    netConn.close();

    for (Thread thr : thrs) {
      thr.join();
    }
    if (failure[0] != null) {
      throw failure[0];
    }
  }

  /**
   * Support for transactions on persistent regions is now enabled for basic
   * case in GemFireXD by default without any transactional recovery etc i.e. only
   * if there is at least one live copy available can data consistency be
   * guaranteed. This is for testing that feature.
   */
  public void _testBasicPersistence() throws Exception {
    //startVMs(1, 3);
    createDiskStore(false, 2);

    final int totalOps = 1000;
    final int txOps = totalOps / 2;
    Connection conn = getClientConnection(false);
    Statement st = conn.createStatement();
    st.execute("create table T1 (PkCol1 int not null, PkCol2 int not null , "
        + "col3 int, col4 int, col5 varchar(10), Primary Key (PkCol1, PkCol2)) "
        + "Partition by column (PkCol1) PERSISTENT '" + DISKSTORE + "'" + getSuffix());
    // create an index
    st.execute("create index col3Index on T1 (col3)");
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    PreparedStatement psInsert = conn.prepareStatement("insert into t1 "
        + "values(?, ?, ?, ?, ?)");
    // first one op per transaction
    for (int i = 0; i < totalOps / 2; i++) {
      psInsert.setInt(1, i);
      psInsert.setInt(2, i);
      psInsert.setInt(3, i);
      psInsert.setInt(4, i);
      psInsert.setString(5, "XXXX" + i);
      psInsert.executeUpdate();
      if ((i % 2) == 0) {
        conn.commit();
      } else {
        conn.rollback();
      }
    }
    // next bunch of ops for a distributed transaction
    for (int i = totalOps / 2; i < totalOps; i += 2) {
      psInsert.setInt(1, i);
      psInsert.setInt(2, i);
      psInsert.setInt(3, i);
      psInsert.setInt(4, i);
      psInsert.setString(5, "XXXX" + i);
      psInsert.executeUpdate();
    }
    conn.commit();
    for (int i = totalOps / 2 + 1; i < totalOps; i += 2) {
      psInsert.setInt(1, i);
      psInsert.setInt(2, i);
      psInsert.setInt(3, i);
      psInsert.setInt(4, i);
      psInsert.setString(5, "XXXX" + i);
      psInsert.executeUpdate();
    }
    conn.rollback();
    // check for results
    ResultSet rs = st.executeQuery("select * from t1");
    int numRows = 0;
    while (rs.next()) {
      numRows++;
    }
    assertEquals("Table should have " + txOps + " rows", txOps, numRows);
    rs.close();
    st.close();
    conn.commit();
    conn.close();

    // now stop VMs and check recovery from disk
    stopVMNums(1);
    stopVMNums(-1, -3, -2);

    // restart VMs
    restartVMNums(-2, -3, -1);
    restartVMNums(1);

    // check for data recovery
    conn = getClientConnection(false);
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    st = conn.createStatement();
    final TIntHashSet results = new TIntHashSet(5000);
    rs = st.executeQuery("select * from t1");
    while (rs.next()) {
      final int pkCol1 = rs.getInt(1);
      assertTrue("unexpected value for PkCol1 " + pkCol1, pkCol1 >= 0
          && pkCol1 < totalOps && (pkCol1 % 2) == 0);
      if (!results.add(pkCol1)) {
        throw new TestException("unexpected duplicate value " + pkCol1);
      }
    }
    assertEquals("Table should have " + txOps + " rows", txOps, results.size());
    rs.close();
    st.close();

    // also check for correct index creation after recovery
    final PreparedStatement pstmt = conn
        .prepareStatement("select * from t1 where col3 = ?");
    for (int index = 0; index < totalOps; ++index) {
      pstmt.setInt(1, index);
      rs = pstmt.executeQuery();
      if (index % 2 == 0) {
        // expect to find data
        assertTrue(rs.next());
        assertEquals(index, rs.getInt(1));
        assertEquals(index, rs.getInt(2));
        assertEquals(index, rs.getInt(3));
        assertEquals(index, rs.getInt(4));
        assertFalse(rs.next());
      } else {
        assertFalse(rs.next());
      }
      rs.close();
    }

    pstmt.close();
    conn.commit();

    conn.createStatement().execute("drop table t1");
    conn.commit();
    conn.close();
  }

  /**
   * Check that transaction continues fine after new node join.
   */
  public void _testNewNodeHA() throws Throwable {
    //startVMs(1, 2);
    Connection conn = getClientConnection(false);
    Statement st = conn.createStatement();
    st.execute("create schema tran");
    st.execute("Create table tran.t1 (c1 int not null , c2 int not null, "
        + "primary key(c1)) partition by primary key " + getSuffix());
    conn.commit();
    conn.setTransactionIsolation(getIsolationLevel());
    st.execute("insert into tran.t1 values (10, 10)");
    st.execute("insert into tran.t1 values (20, 20)");

    final CyclicBarrier barrier = new CyclicBarrier(2);
    final Throwable[] failEx = new Throwable[]{null};
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Connection conn = getClientConnection(false);
          conn.setAutoCommit(false);
          Statement st = conn.createStatement();
          conn.setTransactionIsolation(getIsolationLevel());
          st.execute("insert into tran.t1 values (30, 30)");
          st.execute("insert into tran.t1 values (40, 40)");

          // check successful commit the first time with new client
          barrier.await();
          conn.commit();

          st.execute("update tran.t1 set c2 = 60 where c1 = 30");
          st.execute("update tran.t1 set c2 = 50 where c2 = 40");
          st.execute("insert into tran.t1 values (60, 60)");

          // check successful commit the second time with new store
          barrier.await();
          conn.commit();
          TXManagerImpl.waitForPendingCommitForTest();
        } catch (Throwable t) {
          failEx[0] = t;
          getLogWriter().error("unexpected exception", t);
        }
      }
    });
    t.start();

    // start a new client VM but TXns should still complete without problems
    //startVMs(1, 0);
    // break the barrier
    barrier.await();
    conn.commit();
    if (failEx[0] != null) {
      throw failEx[0];
    }

    st.execute("update tran.t1 set c2 = 50 where c1 = 20");
    st.execute("update tran.t1 set c2 = 40 where c2 = 10");
    st.execute("insert into tran.t1 values (50, 50)");

    // start a new store VM and TXns should continue
    //startVMs(0, 1);

    Connection conn2 = getClientConnection(false);
    Statement st2 = conn2.createStatement();
    st2.execute("call sys.rebalance_all_buckets()");

    // break the barrier
    barrier.await();
    conn.commit();

    t.join();
    if (failEx[0] != null) {
      throw failEx[0];
    }
    TXManagerImpl.waitForPendingCommitForTest();

    // check the final values
    SerializableRunnable checkRS = new SerializableRunnable() {
      @Override
      public void run() {
        try {
          Connection conn = getClientConnection(false);
          Statement st = conn.createStatement();
          ResultSet rs = st.executeQuery("select * from tran.t1");
          Object[][] expectedOutput = new Object[][]{
              new Object[]{10, 40},
              new Object[]{20, 50},
              new Object[]{30, 60},
              new Object[]{40, 50},
              new Object[]{50, 50},
              new Object[]{60, 60},
          };
          JDBC.assertUnorderedResultSet(rs, expectedOutput, false);
        } catch (SQLException sqle) {
          fail("failed with SQLException", sqle);
        }
      }
    };
    clientExecute(1, checkRS);
    clientExecute(2, checkRS);
    serverExecute(1, checkRS);
    serverExecute(2, checkRS);
    serverExecute(3, checkRS);
  }

  public void DISABLED_testWriteLockTimeout() throws Exception {
    // set the timeout on all VMs
    invokeInEveryVM(new SerializableRunnable() {
      @Override
      public void run() {
        System.setProperty("gemfire.WRITE_LOCK_TIMEOUT", "-1");
      }
    });

    startClientVMs(1, 0, null);
    startServerVMs(1, 0, null);

    Connection conn = getClientConnection(false);
    Statement st = conn.createStatement();

    st.execute("CREATE TABLE QUOTE(" +
        "QUOTEID INTEGER NOT NULL," +
        "LOW NUMERIC(14,2)," +
        "OPEN1 NUMERIC(14,2)," +
        "VOLUME NUMERIC(19,2) NOT NULL," +
        "PRICE NUMERIC(14,2)," +
        "HIGH NUMERIC(14,2)," +
        "COMPANYNAME VARCHAR(250)," +
        "SYMBOL VARCHAR(250) NOT NULL," +
        "CHANGE1 NUMERIC(19,2) NOT NULL," +
        "PRIMARY KEY (QUOTEID)" +
        ") REPLICATE PERSISTENT SYNCHRONOUS" + getSuffix());
    st.execute("create index quote_symbol on quote(symbol)");
    conn.commit();
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    final int totalRows = 400;
    PreparedStatement psInsert = conn.prepareStatement("insert into quote "
        + "(quoteid, volume, companyname, symbol, change1) "
        + "values (?, 1, 'VMW', 'VMW', 0)");
    for (int i = 0; i < totalRows; i++) {
      psInsert.setInt(1, i);
      psInsert.executeUpdate();
    }
    conn.commit();

    ResultSet rs = st.executeQuery("select * from quote");
    int numRows = 0;
    while (rs.next()) {
      assertEquals("Column value should be VMW", "VMW",
          rs.getString("COMPANYNAME"));
      assertEquals("Column value should be VMW", "VMW", rs.getString("SYMBOL"));
      assertEquals("Column value should be 0", "0.00", rs.getString("CHANGE1"));
      numRows++;
    }
    assertEquals("Numbers of rows in resultset should be " + totalRows,
        totalRows, numRows);
    conn.commit();

    // now execute updates in parallel threads with conflicts but
    // timeout should avoid failures
    final int numThreads = 5;
    final CyclicBarrier barrier = new CyclicBarrier(numThreads);
    final Exception[] failures = new Exception[1];
    Runnable task = new Runnable() {
      @Override
      public void run() {
        try {
          barrier.await();
          Connection conn = getClientConnection(false);
          conn.setTransactionIsolation(getIsolationLevel());
          conn.setAutoCommit(false);
          PreparedStatement psUpdate = conn
              .prepareStatement("update quote set change1=?, companyname=?, "
                  + "high=?, open1=?, price=?, low=?, volume=? where symbol=?");
          for (int i = 0; i < totalRows; i++) {
            psUpdate.setBigDecimal(1, new BigDecimal("34." + i));
            psUpdate.setString(2, "VMW");
            psUpdate.setBigDecimal(3, new BigDecimal("35.8"));
            psUpdate.setBigDecimal(4, new BigDecimal("34." + i));
            psUpdate.setBigDecimal(5, new BigDecimal("34.3"));
            psUpdate.setBigDecimal(6, new BigDecimal("34.1"));
            psUpdate.setBigDecimal(7, new BigDecimal("0"));
            psUpdate.setString(8, "VMW");
            psUpdate.executeUpdate();
            conn.commit();
          }
        } catch (Exception e) {
          failures[0] = e;
          getLogWriter().error(e);
          fail("unexpected exception", e);
        }
      }
    };

    final Thread[] thrs = new Thread[numThreads];
    for (int i = 0; i < thrs.length; i++) {
      thrs[i] = new Thread(task);
      thrs[i].start();
    }
    for (Thread thr : thrs) {
      thr.join();
    }
    if (failures[0] != null) {
      throw failures[0];
    }

    rs = st.executeQuery("select * from quote");
    numRows = 0;
    while (rs.next()) {
      assertEquals("Column value should change", "35.8", rs.getString("HIGH"));
      assertEquals("Columns value should change", "34.3", rs.getString("PRICE"));
      assertEquals("Column value should change", "34.1", rs.getString("LOW"));
      numRows++;
    }
    assertEquals("Numbers of rows in resultset should be " + totalRows,
        totalRows, numRows);
    rs.close();
    st.close();
    conn.commit();
    conn.close();

    st.execute("drop table quote");

    invokeInEveryVM(new SerializableRunnable() {
      @Override
      public void run() {
        System.setProperty("gemfire.WRITE_LOCK_TIMEOUT", "0");
      }
    });
  }

  public void _testDeltaGII_51366() throws Exception {
    reduceLogLevelForTest("config");
    //startVMs(1, 2);

    Properties props = new Properties();
    props.setProperty("sync-commits", "true");
    Connection conn = TestUtil.getConnection(props);
    Statement stmt = conn.createStatement();
    PreparedStatement pstmt;
    // create a table and global index
    stmt.execute("create table trade.customer ("
        + "  c_w_id         integer        not null primary key,"
        + "  c_d_id         integer        not null,"
        + "  c_id           integer        not null,"
        + "  c_data         varchar(500))"
        + " partition by (c_w_id) redundancy 2 persistent synchronous");
    stmt.execute("create unique index custId on trade.customer(c_id)");

    // now a bunch of inserts
    pstmt = conn
        .prepareStatement("insert into trade.customer values (?, ?, ?, ?)");
    final int numBaseInserts = 1000;
    for (int i = 0; i < numBaseInserts; i++) {
      pstmt.setInt(1, i * 2);
      pstmt.setInt(2, i / 2);
      pstmt.setInt(3, i);
      pstmt.setString(4, "data-" + i);
      pstmt.execute();
    }
    // transactional inserts
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    for (int i = numBaseInserts; i < 2 * numBaseInserts; i++) {
      pstmt.setInt(1, i * 2);
      pstmt.setInt(2, i / 2);
      pstmt.setInt(3, i);
      pstmt.setString(4, "data-" + i);
      pstmt.execute();
    }

    // start a new node in the middle of transaction
    //startVMs(0, 1);
    stmt.execute("call sys.rebalance_all_buckets()");

    for (int i = 2 * numBaseInserts; i < 3 * numBaseInserts; i++) {
      pstmt.setInt(1, i * 2);
      pstmt.setInt(2, i / 2);
      pstmt.setInt(3, i);
      pstmt.setString(4, "data-" + i);
      pstmt.execute();
    }
    conn.commit();

    // check for full GII (one set of TX ops will be received directly)
    verifyDeltaSizeFromStats(getServerVM(3), 2 * numBaseInserts, 113, 0,
        "/TRADE/CUSTOMER");
    verifyDeltaSizeFromStats(getServerVM(3), 2 * numBaseInserts, 113, 0,
        "/TRADE/CUSTID");
    // check data
    verify_test51366(3 * numBaseInserts);

    // stop a server and check data again
    stopVMNum(-2);
    stmt.execute("call sys.rebalance_all_buckets()");
    // check for full GII (one set of TX ops have been received directly)
    verifyDeltaSizeFromStats(getServerVM(3), 2 * numBaseInserts, 113, 0,
        "/TRADE/CUSTOMER");
    verifyDeltaSizeFromStats(getServerVM(3), 2 * numBaseInserts, 113, 0,
        "/TRADE/CUSTID");
    // check data
    verify_test51366(3 * numBaseInserts);

    // do more inserts and transactional inserts
    conn.setTransactionIsolation(Connection.TRANSACTION_NONE);
    for (int i = 3 * numBaseInserts; i < 4 * numBaseInserts; i++) {
      pstmt.setInt(1, i * 2);
      pstmt.setInt(2, i / 2);
      pstmt.setInt(3, i);
      pstmt.setString(4, "data-" + i);
      pstmt.execute();
    }
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    for (int i = 4 * numBaseInserts; i < 5 * numBaseInserts; i++) {
      pstmt.setInt(1, i * 2);
      pstmt.setInt(2, i / 2);
      pstmt.setInt(3, i);
      pstmt.setString(4, "data-" + i);
      pstmt.execute();
    }

    // restart node in the middle of transaction
    // use fabric server property to rebalance (#51584)
    Properties extraProps = new Properties();
    extraProps.setProperty("rebalance", "true");
    restartVMNums(new int[]{-2}, 0, null, extraProps);
    // wait for rebalance to complete
    serverExecute(2, new SerializableRunnable("verify and wait for rebalance") {
      @Override
      public void run() {
        final InternalResourceManager rm = Misc.getGemFireCache()
            .getResourceManager();
        final ResourceManagerStats stats = rm.getStats();
        waitForCriterion(new DistributedTestBase.WaitCriterion() {

          @Override
          public boolean done() {
            return rm.getRebalanceOperations().isEmpty();
          }

          @Override
          public String description() {
            return "waiting for " + rm.getRebalanceOperations()
                + " to complete";
          }
        }, 30000, 10, true);
        if (stats.getRebalancesCompleted() < 1) {
          fail("expected a rebalance to be done");
        }
      }
    });
    //stmt.execute("call sys.rebalance_all_buckets()");

    for (int i = 5 * numBaseInserts; i < 6 * numBaseInserts; i++) {
      pstmt.setInt(1, i * 2);
      pstmt.setInt(2, i / 2);
      pstmt.setInt(3, i);
      pstmt.setString(4, "data-" + i);
      pstmt.execute();
    }
    conn.commit();

    // check for delta GII (one set of TX ops will be received directly)
    verifyDeltaSizeFromStats(getServerVM(2), 2 * numBaseInserts, 113, 113,
        "/TRADE/CUSTOMER");
    verifyDeltaSizeFromStats(getServerVM(2), 2 * numBaseInserts, 113, 113,
        "/TRADE/CUSTID");
    // check data
    verify_test51366(6 * numBaseInserts);

    // stop couple of servers and check data again
    stopVMNum(-1);
    verify_test51366(6 * numBaseInserts);
    stmt.execute("call sys.rebalance_all_buckets()");

    verifyDeltaSizeFromStats(getServerVM(2), 2 * numBaseInserts, 113, 113,
        "/TRADE/CUSTOMER");
    verifyDeltaSizeFromStats(getServerVM(2), 2 * numBaseInserts, 113, 113,
        "/TRADE/CUSTID");
    verifyDeltaSizeFromStats(getServerVM(3), 2 * numBaseInserts, 113, 0,
        "/TRADE/CUSTOMER");
    verifyDeltaSizeFromStats(getServerVM(3), 2 * numBaseInserts, 113, 0,
        "/TRADE/CUSTID");

    stopVMNum(-3);
    verify_test51366(6 * numBaseInserts);
  }

  private void verify_test51366(final int totalInserts)
      throws SQLException, IllegalAccessException, InstantiationException, ClassNotFoundException {
    Connection conn = getClientConnection(false);
    Statement stmt = conn.createStatement();
    ResultSet rs;
    for (int i = 0; i < totalInserts; i++) {
      rs = stmt.executeQuery("select * from trade.customer where c_id=" + i);
      assertTrue("failed for i=" + i, rs.next());
      assertEquals(i * 2, rs.getInt(1));
      assertEquals(i / 2, rs.getInt(2));
      assertEquals(i, rs.getInt(3));
      assertEquals("data-" + i, rs.getString(4));
      assertFalse(rs.next());
      rs.close();
    }
    rs = stmt.executeQuery("select * from trade.customer order by c_id");
    for (int i = 0; i < totalInserts; i++) {
      assertTrue(rs.next());
      assertEquals(i * 2, rs.getInt(1));
      assertEquals(i / 2, rs.getInt(2));
      assertEquals(i, rs.getInt(3));
      assertEquals("data-" + i, rs.getString(4));
    }
    assertFalse(rs.next());
    rs.close();
  }

  public String getSuffix() {
    return "  ";
  }

  protected void checkConnCloseExceptionForReadsOnly(Connection conn)
      throws SQLException {
  }

  protected void checkResultsFor42822(Connection conn, boolean updated)
      throws SQLException {
    PreparedStatement pstmt = conn.prepareStatement("SELECT o_id, o_c_id"
        + " AS maxorderid FROM oorder WHERE o_c_id = ?");
    for (int id = 1; id <= 20; ++id) {
      pstmt.setInt(1, id);
      getLogWriter().info("executing for id=" + id);
      ResultSet rs = pstmt.executeQuery();
      if (updated) {
        final boolean hasNext = rs.next();
        if (hasNext) {
          getLogWriter().info("result for id=" + id + ": " + rs.getInt(1)
              + ", " + rs.getInt(2));
        }
        switch (id) {
          case 3:
            assertTrue(hasNext);
            assertEquals(2, rs.getInt(1));
            assertEquals(3, rs.getInt(2));
            break;
          case 2:
            assertTrue(hasNext);
            assertEquals(4, rs.getInt(1));
            assertEquals(2, rs.getInt(2));
            break;
          case 7:
            assertTrue(hasNext);
            assertEquals(6, rs.getInt(1));
            assertEquals(7, rs.getInt(2));
            break;
          default:
            assertFalse(hasNext);
            getLogWriter().info("no result for id=" + id);
            continue;
        }
      } else {
        if (id <= 4) {
          assertTrue(rs.next());
          getLogWriter().info("result for id=" + id + ": " + rs.getInt(1)
              + ", " + rs.getInt(2));
          assertEquals(id * 2, rs.getInt(1));
          assertEquals(id, rs.getInt(2));
        } else {
          assertFalse(rs.next());
          getLogWriter().info("no result for id=" + id);
        }
      }
    }
  }

  public void createDiskStore(boolean useClient, int vmNum) throws Exception {
    SerializableRunnable csr = DistributedSQLTestBase.getDiskStoreCreator(DISKSTORE);
    if (useClient) {
      if (vmNum == 1) {
        csr.run();
      } else {
        clientExecute(vmNum, csr);
      }
    } else {
      serverExecute(vmNum, csr);
    }
  }

  public static void installObservers() {
    final CyclicBarrier testBarrier = new CyclicBarrier(2);
    final ConcurrentHashMap<TXStateProxy, Boolean> waitDone =
        new ConcurrentHashMap<TXStateProxy, Boolean>(2);

    TransactionObserver txOb1 = new TransactionObserverAdapter() {
      boolean firstCall = true;

      @Override
      public void beforeIndividualLockUpgradeInCommit(TXStateProxy tx,
          TXEntryState entry) {
        if (this.firstCall) {
          this.firstCall = false;
          return;
        }
        if (waitDone.putIfAbsent(tx, Boolean.TRUE) == null) {
          SanityManager.DEBUG_PRINT("info:TEST",
              "TXObserver: waiting on testBarrier, count="
                  + testBarrier.getNumberWaiting());
          try {
            testBarrier.await();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }

      @Override
      public void afterIndividualRollback(TXStateProxy tx, Object callbackArg) {
        // release the barrier for the committing TX
        if (waitDone.putIfAbsent(tx, Boolean.TRUE) == null) {
          try {
            testBarrier.await();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }
    };

    GemFireXDQueryObserver ob2 = new GemFireXDQueryObserverAdapter() {
      @Override
      public void lockingRowForTX(TXStateProxy tx, GemFireContainer container,
          RegionEntry entry, boolean writeLock) {
        if (!writeLock
            && ExclusiveSharedSynchronizer.isExclusive(entry.getState())
            && waitDone.putIfAbsent(tx, Boolean.TRUE) == null) {
          SanityManager.DEBUG_PRINT("info:TEST",
              "GFXDObserver: waiting on testBarrier, count="
                  + testBarrier.getNumberWaiting());
          try {
            testBarrier.await();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }
    };

    final TXManagerImpl txMgr = GemFireCacheImpl.getExisting().getTxManager();
    for (TXStateProxy tx : txMgr.getHostedTransactionsInProgress()) {
      tx.setObserver(txOb1);
    }
    txMgr.setObserver(txOb1);
    GemFireXDQueryObserverHolder.setInstance(ob2);
  }

  protected int getIsolationLevel() {
    return Connection.TRANSACTION_READ_COMMITTED;
  }

  public void _test49667() throws Exception {
    //startVMs(1, 2);
    Connection conn = getClientConnection(false);
    clientSQLExecute(
        1,
        "create table trade.portfolio (cid int not null, sid int not null, qty int not null, "
            + "availQty int not null, constraint portf_pk primary key (cid, sid))");
    clientSQLExecute(
        1,
        "create table trade.sellorders (oid int not null constraint orders_pk primary key, "
            + "cid int, sid int, qty int, constraint portf_fk foreign key (cid, sid) references trade.portfolio (cid, sid) on delete restrict)");

    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    Statement stmt = conn.createStatement();

    stmt.execute("insert into trade.portfolio values(1, 1, 1, 1)");
    conn.commit();

    stmt = conn.createStatement();
    stmt.execute("insert into trade.sellorders values(1, 1, 1, 1)");

    // stmt.execute("update trade.sellorders set qty = 100 where oid = 1");
    try {
      addExpectedException(new int[]{1}, new int[]{1},
          ConflictException.class);
      stmt.execute("insert into trade.portfolio values(1, 1, 10, 10)");
      fail("the above insert should have failed");
    } catch (Exception sqle) {
      sqle.printStackTrace();
//      TestUtil.getLogger().info("exception state is: " + sqle.getSQLState(),
//          sqle);
//      if (!"X0Z02".equals(sqle.getSQLState())) {
//        throw sqle;
//      }
    }
    removeExpectedException(new int[]{}, new int[]{}, ConflictException.class);
    stmt.close();
    conn.rollback();

    Connection newConn = getClientConnection(false);
    newConn.setTransactionIsolation(getIsolationLevel());
    newConn.setAutoCommit(false);
    Statement newstmt = newConn.createStatement();
    newstmt.execute("insert into trade.sellorders values(2, 1, 1, 1)");

    newstmt.close();
    newConn.commit();
  }

  /**
   * Test insufficient data store behaviour for distributed/update/delete/select
   * and for primary key based select/update/delete
   *
   * @throws Exception
   */
  public void _testGFXDDeleteWithConcurrency() throws Exception {
    //startVMs(0, 2);
    //startVMs(1, 0);
    createDiskStore(true, 1);
    // Create a schema
    clientSQLExecute(1, "create schema trade");

    Map<Integer, String> expected = new HashMap<Integer, String>();
    clientSQLExecute(1, "create table trade.customers (cid int not null, "
        + "cust_name varchar(100), tid int, primary key (cid)) ENABLE CONCURRENCY CHECKS replicate "
        + getSuffix());
    Connection conn = getClientConnection(false);
    conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    conn.setAutoCommit(false);
    PreparedStatement psInsert = conn
        .prepareStatement("insert into trade.customers values (?,?,?)");
    for (int i = 1; i < 5; ++i) {
      psInsert.setInt(1, i);
      psInsert.setString(2, "unmodified");
      psInsert.setInt(3, i);
      psInsert.executeUpdate();
      expected.put(i, "unmodified");
    }
    conn.commit();
    Statement st = conn.createStatement();
    boolean b = st.execute("delete from trade.customers where cid = 4");
    conn.commit();
    expected.remove(4);

    {
      //Make sure vm2 has the correct contents.
      //Now we want to validate the region contents and RVVs...
      Statement s = conn.createStatement();
      s.execute("select * from trade.customers");
      ResultSet rs = s.getResultSet();

      Map<Integer, String> received = new HashMap();
      while (rs.next()) {
        received.put(rs.getInt("cid"), rs.getString("cust_name"));
      }

      assertEquals(expected, received);
    }

    stopVMNums(-1);

    //Make sure vm2 has the correct contents.
    //Now we want to validate the region contents and RVVs...
    {
      Statement s = conn.createStatement();
      s.execute("select * from trade.customers");
      ResultSet rs = s.getResultSet();
      rs = s.getResultSet();

      Map<Integer, String> received = new HashMap();
      while (rs.next()) {
        received.put(rs.getInt("cid"), rs.getString("cust_name"));
      }
      assertEquals(expected, received);
    }
  }
}
