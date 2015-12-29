package io.snappydata.impl;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import com.gemstone.gemfire.internal.LogWriterImpl;
import com.pivotal.gemfirexd.internal.catalog.ExternalCatalog;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.impl.jdbc.Util;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.GfxdDataDictionary;
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.spark.sql.hive.ExternalTableType;
import org.apache.thrift.TException;

/**
 * Encapsulates the hive catalog as created by Snappy Spark.
 * <p/>
 * Created by kneeraj on 10/1/15.
 */
public class SnappyHiveCatalog implements ExternalCatalog {

  final private static String THREAD_GROUP_NAME = "HiveMetaStore Client Group";

  private ThreadLocal<HiveMetaStoreClient> hmClients = new ThreadLocal<>();

  private final ThreadLocal<HMSQuery> queries = new ThreadLocal<>();

  private final ExecutorService hmsQueriesExecutorService;

  private final static String DEFAULT_DB_NAME = "default";

  private final ArrayList<HiveMetaStoreClient> allHMclients = new ArrayList<>();

  public SnappyHiveCatalog() {
    final ThreadGroup hmsThreadGroup = LogWriterImpl.createThreadGroup(
        THREAD_GROUP_NAME, Misc.getI18NLogWriter());
    ThreadFactory hmsClientThreadFactory = new ThreadFactory() {
      private int next = 0;

      public Thread newThread(Runnable command) {
        Thread t = new Thread(hmsThreadGroup, command, "HiveMetaStore Client-"
            + next++);
        t.setDaemon(true);
        return t;
      }
    };
    hmsQueriesExecutorService = Executors.newFixedThreadPool(1, hmsClientThreadFactory);
    // just run a task to initialize the HMC for the thread. Assumption is that this should be outside
    // any lock
    HMSQuery q = getHMSQuery();
    q.resetValues(HMSQuery.INIT, null, null, true);
    Future<Boolean> ret = hmsQueriesExecutorService.submit(q);
    try {
      ret.get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public boolean isColumnTable(String tableName, boolean skipLocks) {
    HMSQuery q = getHMSQuery();
    q.resetValues(HMSQuery.ISCOLUMNTABLE_QUERY, tableName, DEFAULT_DB_NAME, skipLocks);
    Future<Boolean> f = this.hmsQueriesExecutorService.submit(q);
    return handleFutureResult(f);
  }

  public boolean isRowTable(String tableName, boolean skipLocks) {
    HMSQuery q = getHMSQuery();
    q.resetValues(HMSQuery.ISROWTABLE_QUERY, tableName, DEFAULT_DB_NAME, skipLocks);
    Future<Boolean> f = this.hmsQueriesExecutorService.submit(q);
    return handleFutureResult(f);
  }

  @Override
  public void stop() {
    for (HiveMetaStoreClient cl : this.allHMclients) {
      cl.close();
    }
    this.hmClients = null;
    this.allHMclients.clear();
    this.hmsQueriesExecutorService.shutdown();
  }

  private HMSQuery getHMSQuery() {
    HMSQuery q = this.queries.get();
    if (q == null) {
      q = new HMSQuery();
      this.queries.set(q);
    }
    return q;
  }

  private boolean handleFutureResult(Future<Boolean> f) {
    try {
      return f.get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private class HMSQuery implements Callable<Boolean> {

    private int qType;
    private String tableName;
    private String dbName;
    private boolean skipLock;

    private static final int INIT = 0;
    private static final int ISROWTABLE_QUERY = 1;
    private static final int ISCOLUMNTABLE_QUERY = 2;
    private static final int CLOSE_HMC = 3;

    // More to be added later

    HMSQuery() {
    }

    public void resetValues(int queryType, String tableName, String dbName, boolean skipLocks) {
      this.qType = queryType;
      this.tableName = tableName;
      this.dbName = dbName;
      this.skipLock = skipLocks;
    }

    @Override
    public Boolean call() throws Exception {
      try {
        if (this.skipLock) {
          GfxdDataDictionary.SKIP_LOCKS.set(true);
        }
      switch (this.qType) {
        case INIT:
          initHMC();
          return true;

        case ISROWTABLE_QUERY:
          HiveMetaStoreClient hmc = SnappyHiveCatalog.this.hmClients.get();
          String type = getType(hmc);
          return type.equals(ExternalTableType.Row().toString());

        case ISCOLUMNTABLE_QUERY:
          hmc = SnappyHiveCatalog.this.hmClients.get();
          type = getType(hmc);
          return type.equals(ExternalTableType.Columnar().toString());

        default:
          throw new IllegalStateException("HiveMetaStoreClient:unknown query option");
      }
      } finally {
        GfxdDataDictionary.SKIP_LOCKS.set(false);
      }
    }

    public String toString() {
      return "HiveMetaStoreQuery:query type = " + this.qType + " tname = " +
          this.tableName + " db = " + this.dbName;
    }

    private void initHMC() {
      // for unit tests we are not using default-persistent=true as tables of different types
      // with same test name is causing problems with Hive metastore configured with gemfirexd.
      // Need to see proper cleanup of the metastore entries between tests. Will put a proper
      // cleanup soon.
      boolean snappyFunSuite = Boolean.getBoolean("scalaTest");
      String url = "jdbc:snappydata:;user=HIVE_METASTORE;disable-streaming=true"
          + (snappyFunSuite ? "" : ";default-persistent=true");
      HiveConf metadataConf = new HiveConf();
      metadataConf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, url);
      metadataConf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER,
          "com.pivotal.gemfirexd.jdbc.EmbeddedDriver");

      try {
        HiveMetaStoreClient hmc = new HiveMetaStoreClient(metadataConf);
        SnappyHiveCatalog.this.hmClients.set(hmc);
        SnappyHiveCatalog.this.allHMclients.add(hmc);
      } catch (MetaException me) {
        throw new IllegalStateException(me);
      }
    }

    private String getType(HiveMetaStoreClient hmc) throws SQLException {
      try {
        Table t = hmc.getTable(this.dbName, this.tableName);
        return t.getParameters().get("EXTERNAL");
      } catch (NoSuchObjectException nsoe) {
        // assume ROW type in GemFireXD
        return ExternalTableType.Row().toString();
      } catch (TException te) {
        throw Util.generateCsSQLException(SQLState.TABLE_NOT_FOUND,
            this.tableName, te);
      }
    }
  }
}
