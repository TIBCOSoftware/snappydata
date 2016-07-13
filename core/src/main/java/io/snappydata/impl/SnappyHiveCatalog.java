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
import io.snappydata.Constant;
import io.snappydata.util.ServiceUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.collection.Utils;
import org.apache.spark.sql.hive.ExternalTableType;
import org.apache.spark.sql.hive.QualifiedTableName;
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog;
import org.apache.spark.sql.store.StoreUtils;
import org.apache.spark.sql.types.StructType;
import org.apache.thrift.TException;

public class SnappyHiveCatalog implements ExternalCatalog {

  final private static String THREAD_GROUP_NAME = "HiveMetaStore Client Group";

  private ThreadLocal<HiveMetaStoreClient> hmClients = new ThreadLocal<>();

  private final ThreadLocal<HMSQuery> queries = new ThreadLocal<>();

  private final ExecutorService hmsQueriesExecutorService;

  private final ArrayList<HiveMetaStoreClient> allHMclients = new ArrayList<>();

  public SnappyHiveCatalog() {
    final ThreadGroup hmsThreadGroup = LogWriterImpl.createThreadGroup(
        THREAD_GROUP_NAME, Misc.getI18NLogWriter());
    ThreadFactory hmsClientThreadFactory = new ThreadFactory() {
      private int next = 0;

      @SuppressWarnings("NullableProblems")
      public Thread newThread(Runnable command) {
        Thread t = new Thread(hmsThreadGroup, command, "HiveMetaStore Client-"
            + next++);
        t.setDaemon(true);
        return t;
      }
    };
    hmsQueriesExecutorService = Executors.newFixedThreadPool(1, hmsClientThreadFactory);
    // just run a task to initialize the HMC for the thread.
    // Assumption is that this should be outside any lock
    HMSQuery q = getHMSQuery();
    q.resetValues(HMSQuery.INIT, null, null, true);
    Future<Object> ret = hmsQueriesExecutorService.submit(q);
    try {
      ret.get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public boolean isColumnTable(String schema, String tableName, boolean skipLocks) {
    HMSQuery q = getHMSQuery();
    q.resetValues(HMSQuery.ISCOLUMNTABLE_QUERY, tableName, schema, skipLocks);
    Future<Object> f = this.hmsQueriesExecutorService.submit(q);
    return (Boolean)handleFutureResult(f);
  }

  public boolean isRowTable(String schema, String tableName, boolean skipLocks) {
    HMSQuery q = getHMSQuery();
    q.resetValues(HMSQuery.ISROWTABLE_QUERY, tableName, schema, skipLocks);
    Future<Object> f = this.hmsQueriesExecutorService.submit(q);
    return (Boolean)handleFutureResult(f);
  }

  public String getColumnTableSchemaAsJson(String schema, String tableName,
      boolean skipLocks) {
    HMSQuery q = getHMSQuery();
    q.resetValues(HMSQuery.COLUMNTABLE_SCHEMA, tableName, schema, skipLocks);
    Future<Object> f = this.hmsQueriesExecutorService.submit(q);
    return (String)handleFutureResult(f);
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

  private <T> T handleFutureResult(Future<T> f) {
    try {
      return f.get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private class HMSQuery implements Callable<Object> {

    private int qType;
    private String tableName;
    private String dbName;
    private boolean skipLock;

    private static final int INIT = 0;
    private static final int ISROWTABLE_QUERY = 1;
    private static final int ISCOLUMNTABLE_QUERY = 2;
    private static final int COLUMNTABLE_SCHEMA = 3;
    // private static final int CLOSE_HMC = 4;

    // More to be added later

    HMSQuery() {
    }

    public void resetValues(int queryType, String tableName,
        String dbName, boolean skipLocks) {
      this.qType = queryType;
      this.tableName = tableName;
      this.dbName = dbName;
      this.skipLock = skipLocks;
    }

    @Override
    public Object call() throws Exception {
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
          return type.equalsIgnoreCase(ExternalTableType.Row().toString());

        case ISCOLUMNTABLE_QUERY:
          hmc = SnappyHiveCatalog.this.hmClients.get();
          type = getType(hmc);
          return !type.equalsIgnoreCase(ExternalTableType.Row().toString());

        case COLUMNTABLE_SCHEMA:
          hmc = SnappyHiveCatalog.this.hmClients.get();
          return getSchema(hmc);

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
      String url = "jdbc:snappydata:;user=" +
          SnappyStoreHiveCatalog.HIVE_METASTORE() +
          ";disable-streaming=true;default-persistent=true";
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

    private Table getTable(HiveMetaStoreClient hmc,
        String dbName, String tableName) throws SQLException {
      try {
        return hmc.getTable(dbName, tableName);
      } catch (NoSuchObjectException ignored) {
        return null;
      } catch (TException te) {
        throw Util.generateCsSQLException(SQLState.TABLE_NOT_FOUND,
            tableName, te);
      }
    }

    private String getType(HiveMetaStoreClient hmc) throws SQLException {
      Table t = getTable(hmc, this.dbName, this.tableName);
      if (t != null) {
        return t.getParameters().get("EXTERNAL");
      } else {
        // assume ROW type in GemFireXD
        return ExternalTableType.Row().toString();
      }
    }

    private String getSchema(HiveMetaStoreClient hmc) throws SQLException {
      Table table = null;
      try {
        table = getTable(hmc, this.dbName, this.tableName);
      } catch (SQLException sqle) {
        // try with upper-case name
      }
      if (table == null) {
        table = getTable(hmc, this.dbName, Utils.toUpperCase(this.tableName));
      }
      if (table != null) {
        return SnappyStoreHiveCatalog.getSchemaStringFromHiveTable(table);
      } else {
        return null;
      }
    }
  }
}
