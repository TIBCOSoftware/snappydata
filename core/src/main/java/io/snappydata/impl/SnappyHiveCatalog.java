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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.internal.LogWriterImpl;
import com.gemstone.gemfire.internal.cache.ExternalTableMetaData;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.pivotal.gemfirexd.Attribute;
import com.pivotal.gemfirexd.internal.catalog.ExternalCatalog;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.impl.jdbc.Util;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.GfxdDataDictionary;
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState;
import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.spark.sql.collection.Utils;
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils;
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry;
import org.apache.spark.sql.hive.ExternalTableType;
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog;
import org.apache.spark.sql.sources.JdbcExtendedUtils;
import org.apache.spark.sql.store.StoreUtils;
import org.apache.spark.sql.types.StructType;
import org.apache.thrift.TException;

public class SnappyHiveCatalog implements ExternalCatalog {

  final private static String THREAD_GROUP_NAME = "HiveMetaStore Client Group";

  private ThreadLocal<HiveMetaStoreClient> hmClients = new ThreadLocal<>();

  public static final ThreadLocal<Boolean> SKIP_HIVE_TABLE_CALLS =
      new ThreadLocal<>();

  private final ExecutorService hmsQueriesExecutorService;

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
    q.resetValues(HMSQuery.INIT, null, null, false);
    Future<Object> ret = hmsQueriesExecutorService.submit(q);
    try {
      ret.get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static String setCommonHiveMetastoreProperties(HiveConf metadataConf) {
    metadataConf.set("datanucleus.mapping.Schema", Misc.SNAPPY_HIVE_METASTORE);
    // Tomcat pool has been shown to work best but does not work in split mode
    // because upstream spark does not ship with it (and the one in snappydata-core
    //   cannot be loaded by datanucleus which should apparently be in its CLASSPATH)
    metadataConf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_POOLING_TYPE, "BONECP");
    // every session has own hive client, so a small pool
    metadataConf.set("datanucleus.connectionPool.maxPoolSize", "4");
    metadataConf.set("datanucleus.connectionPool.minPoolSize", "0");
    String warehouse = metadataConf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname);
    if (warehouse == null || warehouse.isEmpty() ||
        warehouse.equals(HiveConf.ConfVars.METASTOREWAREHOUSE.getDefaultExpr())) {
      // append warehouse to current directory
      warehouse = new java.io.File("./warehouse").getAbsolutePath();
      metadataConf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, warehouse);
    }
    metadataConf.setVar(HiveConf.ConfVars.HADOOPFS, "file:///");
    return warehouse;
  }

  public Table getTable(String schema, String tableName, boolean skipLocks) {
    HMSQuery q = getHMSQuery();
    q.resetValues(HMSQuery.GET_TABLE, tableName, schema, skipLocks);
    Future<Object> f = this.hmsQueriesExecutorService.submit(q);
    return (Table)handleFutureResult(f);
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

  public List<ExternalTableMetaData> getHiveTables(boolean skipLocks) {
    // skip if this is already the catalog lookup thread (Hive dropTable
    //   invokes getTables again)
    if (Boolean.TRUE.equals(
        SKIP_HIVE_TABLE_CALLS.get())) {
      return Collections.emptyList();
    }
    HMSQuery q = getHMSQuery();
    q.resetValues(HMSQuery.GET_HIVE_TABLES, null, null, skipLocks);
    Future<Object> f = this.hmsQueriesExecutorService.submit(q);
    // noinspection unchecked
    return (List<ExternalTableMetaData>)handleFutureResult(f);
  }

  public String getColumnTableSchemaAsJson(String schema, String tableName,
      boolean skipLocks) {
    HMSQuery q = getHMSQuery();
    q.resetValues(HMSQuery.COLUMNTABLE_SCHEMA, tableName, schema, skipLocks);
    Future<Object> f = this.hmsQueriesExecutorService.submit(q);
    return (String)handleFutureResult(f);
  }

  public ExternalTableMetaData getHiveTableMetaData(String schema, String tableName,
      boolean skipLocks) {
    HMSQuery q = getHMSQuery();
    q.resetValues(HMSQuery.GET_COL_TABLE, tableName, schema, skipLocks);
    Future<Object> f = this.hmsQueriesExecutorService.submit(q);
    return (ExternalTableMetaData)handleFutureResult(f);
  }

  public HashMap<String, List<String>> getAllStoreTablesInCatalog(boolean skipLocks) {
    HMSQuery q = getHMSQuery();
    q.resetValues(HMSQuery.GET_ALL_TABLES_MANAGED_IN_DD, null, null, skipLocks);
    Future<Object> f = this.hmsQueriesExecutorService.submit(q);
    // noinspection unchecked
    return (HashMap<String, List<String>>)handleFutureResult(f);
  }

  public boolean removeTable(String schema,
      String table, boolean skipLocks) {
    HMSQuery q = getHMSQuery();
    q.resetValues(HMSQuery.REMOVE_TABLE, table, schema, skipLocks);
    Future<Object> f = this.hmsQueriesExecutorService.submit(q);
    return (Boolean)handleFutureResult(f);
  }

  @Override
  public String catalogSchemaName() {
    return SnappyStoreHiveCatalog.HIVE_METASTORE();
  }

  @Override
  public void stop() {
    HMSQuery q = getHMSQuery();
    q.resetValues(HMSQuery.CLOSE_HMC, null, null, true);
    try {
      this.hmsQueriesExecutorService.submit(q).get();
    } catch (Exception ignored) {
    }
    this.hmsQueriesExecutorService.shutdown();
    try {
      this.hmsQueriesExecutorService.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException ignored) {
    }
  }

  private HMSQuery getHMSQuery() {
    return new HMSQuery();
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
    // all hive tables that are expected to be in datadictionary
    // this will exclude external tables like parquet tables, stream tables
    private static final int GET_ALL_TABLES_MANAGED_IN_DD = 4;
    private static final int REMOVE_TABLE = 5;
    private static final int GET_COL_TABLE = 6;
    private static final int CLOSE_HMC = 7;
    private static final int GET_TABLE = 8;
    private static final int GET_HIVE_TABLES = 9;

    // More to be added later

    HMSQuery() {
    }

    private void resetValues(int queryType, String tableName,
        String dbName, boolean skipLocks) {
      this.qType = queryType;
      this.tableName = tableName;
      this.dbName = dbName;
      this.skipLock = skipLocks;
    }

    @Override
    public Object call() throws Exception {
      SKIP_HIVE_TABLE_CALLS.set(Boolean.TRUE);
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
          return type.equalsIgnoreCase(ExternalTableType.Row().name());

        case ISCOLUMNTABLE_QUERY:
          hmc = SnappyHiveCatalog.this.hmClients.get();
          type = getType(hmc);
          return !type.equalsIgnoreCase(ExternalTableType.Row().name());

        case COLUMNTABLE_SCHEMA:
          hmc = SnappyHiveCatalog.this.hmClients.get();
          return getSchema(hmc);

        case GET_TABLE:
          hmc = SnappyHiveCatalog.this.hmClients.get();
          return getTable(hmc, this.dbName, this.tableName);

        case GET_HIVE_TABLES: {
          hmc = SnappyHiveCatalog.this.hmClients.get();
          List<String> schemas = hmc.getAllDatabases();
          ArrayList<ExternalTableMetaData> externalTables = new ArrayList<>();
          for (String schema : schemas) {
            List<String> tables = hmc.getAllTables(schema);
            for (String tableName : tables) {
              Table table = hmc.getTable(schema, tableName);
              String tableType = table.getParameters().get(
                  JdbcExtendedUtils.TABLETYPE_PROPERTY());
              if (!ExternalTableType.Row().name().equalsIgnoreCase(tableType)) {
                // TODO: FIX ME: should not convert to upper case blindly
                // but unfortunately hive meta-store is not case-sensitive
                ExternalTableMetaData metaData = new ExternalTableMetaData(
                    Utils.toUpperCase(table.getTableName()),
                    Utils.toUpperCase(table.getDbName()),
                    tableType, null, -1, -1,
                    null, null, null, null);
                metaData.provider = table.getParameters().get(
                    SnappyStoreHiveCatalog.HIVE_PROVIDER());
                metaData.columns = ExternalStoreUtils.getColumnMetadata(
                    ExternalStoreUtils.getTableSchema(table.getParameters()));
                externalTables.add(metaData);
              }
            }
          }
          return externalTables;
        }

        case GET_ALL_TABLES_MANAGED_IN_DD:
          hmc = SnappyHiveCatalog.this.hmClients.get();
          List<String> dbList = hmc.getAllDatabases();
          HashMap<String, List<String>> dbTablesMap = new HashMap<>();
          for (String db : dbList) {
            List<String> tables = hmc.getAllTables(db);
            // TODO: FIX ME: should not convert to upper case blindly
            List <String> upperCaseTableNames = new LinkedList<>();
            for (String t : tables) {
              Table hiveTab = hmc.getTable(db, t);
              String tableType = hiveTab.getParameters().get(
                  JdbcExtendedUtils.TABLETYPE_PROPERTY());
              if (isTableInStoreDD(tableType)) {
                upperCaseTableNames.add(Utils.toUpperCase(t));
              }
            }
            dbTablesMap.put(Utils.toUpperCase(db), upperCaseTableNames);
          }
          return dbTablesMap;
        case REMOVE_TABLE:
          hmc = SnappyHiveCatalog.this.hmClients.get();
          hmc.dropTable(this.dbName, this.tableName);
          return true;
        case GET_COL_TABLE:
          hmc = SnappyHiveCatalog.this.hmClients.get();
          Table table = getTableWithRetry(hmc);
          if (table == null) return null;
          String fullyQualifiedName = Utils.toUpperCase(table.getDbName()) +
              "." + Utils.toUpperCase(table.getTableName());
          StructType schema = ExternalStoreUtils.getTableSchema(
              table.getParameters()).get();
          @SuppressWarnings("unchecked")
          Map<String, String> parameters = new CaseInsensitiveMap(
              table.getSd().getSerdeInfo().getParameters());
          int partitions = ExternalStoreUtils.getAndSetTotalPartitions(
              parameters, true);
          Object value = parameters.get(StoreUtils.GEM_INDEXED_TABLE());
          String baseTable = value != null ? value.toString() : "";
          String dmls = JdbcExtendedUtils.
              getInsertOrPutString(fullyQualifiedName, schema, false, false);
          value = parameters.get(ExternalStoreUtils.DEPENDENT_RELATIONS());
          String[] dependentRelations = value != null
              ? value.toString().split(",") : null;
          int columnBatchSize = Integer.parseInt(parameters.get(
              ExternalStoreUtils.COLUMN_BATCH_SIZE()));
          int columnMaxDeltaRows = Integer.parseInt(parameters.get(
              ExternalStoreUtils.COLUMN_MAX_DELTA_ROWS()));
          value = parameters.get(ExternalStoreUtils.COMPRESSION_CODEC());
          String compressionCodec = value == null ? null : value.toString();
          String tableType = table.getParameters().get(
              JdbcExtendedUtils.TABLETYPE_PROPERTY());
        return new ExternalTableMetaData(
              fullyQualifiedName,
              schema,
              tableType,
              ExternalStoreUtils.getExternalStoreOnExecutor(parameters,
                  partitions, fullyQualifiedName, schema),
              columnBatchSize,
              columnMaxDeltaRows,
              compressionCodec,
              baseTable,
              dmls,
              dependentRelations);

        case CLOSE_HMC:
          hmc = SnappyHiveCatalog.this.hmClients.get();
          hmc.close();
          SnappyHiveCatalog.this.hmClients.remove();
          return true;

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
      DriverRegistry.register("io.snappydata.jdbc.EmbeddedDriver");
      DriverRegistry.register("io.snappydata.jdbc.ClientDriver");

      HiveConf metadataConf = new HiveConf();
      String urlSecure = "jdbc:snappydata:" +
          ";user=" + SnappyStoreHiveCatalog.HIVE_METASTORE() +
          ";disable-streaming=true;default-persistent=true;internal-connection=true";
      final Map<Object, Object> bootProperties = Misc.getMemStore().getBootProperties();
      if (bootProperties.containsKey(Attribute.USERNAME_ATTR) && bootProperties.containsKey
          (Attribute.PASSWORD_ATTR)) {
        urlSecure = "jdbc:snappydata:" +
            ";user=" + bootProperties.get(Attribute.USERNAME_ATTR) +
            ";password=" + bootProperties.get(Attribute.PASSWORD_ATTR) +
            ";default-schema=" + SnappyStoreHiveCatalog.HIVE_METASTORE() +
            ";disable-streaming=true;default-persistent=true;internal-connection=true";
        /*
        metadataConf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME,
            bootProperties.get("user").toString());
        metadataConf.setVar(HiveConf.ConfVars.METASTOREPWD,
            bootProperties.get("password").toString());
        */
      } else {
        metadataConf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME,
            Misc.SNAPPY_HIVE_METASTORE);
      }
      setCommonHiveMetastoreProperties(metadataConf);
      metadataConf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, urlSecure);
      metadataConf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER,
          "io.snappydata.jdbc.EmbeddedDriver");

      final short numRetries = 40;
      short count = 0;
      while (true) {
        try {
          HiveMetaStoreClient hmc = new HiveMetaStoreClient(metadataConf);
          SnappyHiveCatalog.this.hmClients.set(hmc);
          // a dummy table query to pre-initialize most of hive metastore tables
          try {
            getTable(hmc, "APP", "DUMMY");
          } catch (SQLException ignored) {
          }
          return;
        } catch (Exception ex) {
          Throwable t = ex;
          boolean noDataStoreFound = false;
          while (t != null && !noDataStoreFound) {
            noDataStoreFound = (t instanceof SQLException) &&
                SQLState.NO_DATASTORE_FOUND.startsWith(
                    ((SQLException)t).getSQLState());
            t = t.getCause();
          }
          // wait for some time and retry if no data store found error
          // is thrown due to region not being initialized
          if (count < numRetries && noDataStoreFound) {
            try {
              Misc.getI18NLogWriter().warning(LocalizedStrings.DEBUG,
                  "SnappyHiveCatalog.HMSQuery.initHMC: No datastore found " +
                      "while initializing Hive metastore client. " +
                      "Will retry initialization after 3 seconds. " +
                      "Exception received is " + t);
              if (Misc.getI18NLogWriter().fineEnabled()) {
                Misc.getI18NLogWriter().warning(LocalizedStrings.DEBUG,
                    "Exception stacktrace:", ex);
              }
              count++;
              Thread.sleep(3000);
            } catch (InterruptedException ie) {
              throw new IllegalStateException(ex);
            }
          } else {
            throw new IllegalStateException(ex);
          }
        }
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
        return t.getParameters().get(JdbcExtendedUtils.TABLETYPE_PROPERTY());
      } else {
        // assume ROW type in GemFireXD
        return ExternalTableType.Row().name();
      }
    }

    private boolean isTableInStoreDD(String type) {
      return type.equalsIgnoreCase(ExternalTableType.Row().name()) ||
          type.equalsIgnoreCase(ExternalTableType.Column().name()) ||
          type.equalsIgnoreCase(ExternalTableType.Sample().name());
    }

    private Table getTableWithRetry(HiveMetaStoreClient hmc) throws SQLException {
      Table table = null;
      try {
        table = getTable(hmc, this.dbName, this.tableName);
      } catch (SQLException sqle) {
        // try with upper-case name
      }
      if (table == null) {
        table = getTable(hmc, this.dbName, Utils.toUpperCase(this.tableName));
      }
      return table;
    }

    private String getSchema(HiveMetaStoreClient hmc) throws SQLException {
      Table table = getTableWithRetry(hmc);
      if (table != null) {
        return SnappyStoreHiveCatalog.getSchemaStringFromHiveTable(table);
      } else {
        return null;
      }
    }
  }
}
