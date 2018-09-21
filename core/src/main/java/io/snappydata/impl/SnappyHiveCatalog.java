/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.internal.GFToSlf4jBridge;
import com.gemstone.gemfire.internal.LogWriterImpl;
import com.gemstone.gemfire.internal.cache.ExternalTableMetaData;
import com.gemstone.gemfire.internal.cache.GemfireCacheHelper;
import com.gemstone.gemfire.internal.cache.PolicyTableData;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.shared.SystemProperties;
import com.pivotal.gemfirexd.Attribute;
import com.pivotal.gemfirexd.internal.catalog.ExternalCatalog;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.diag.HiveTablesVTI;
import com.pivotal.gemfirexd.internal.engine.locks.GfxdDRWLockService;
import com.pivotal.gemfirexd.internal.engine.locks.GfxdLockSet;
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore;
import com.pivotal.gemfirexd.internal.impl.jdbc.Util;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.GfxdDataDictionary;
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState;
import io.snappydata.Constant;
import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SnappyContext;
import org.apache.spark.sql.collection.Utils;
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils;
import org.apache.spark.sql.hive.ExternalTableType;
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog;
import org.apache.spark.sql.policy.PolicyProperties;
import org.apache.spark.sql.sources.JdbcExtendedUtils;
import org.apache.spark.sql.store.StoreUtils;
import org.apache.spark.sql.types.StructType;

public class SnappyHiveCatalog implements ExternalCatalog {

  final private static String THREAD_GROUP_NAME = "HiveMetaStore Client Group";

  private final Future<?> initFuture;

  public static final Object hiveClientSync = new Object();

  private final ExecutorService hmsQueriesExecutorService;

  public SnappyHiveCatalog() {
    final ThreadGroup hmsThreadGroup = LogWriterImpl.createThreadGroup(
        THREAD_GROUP_NAME, Misc.getI18NLogWriter());
    ThreadFactory hmsClientThreadFactory = GemfireCacheHelper.createThreadFactory(
        hmsThreadGroup, "HiveMetaStore Client");
    hmsQueriesExecutorService = Executors.newFixedThreadPool(1, hmsClientThreadFactory);
    // just run a task to initialize the HMC for the thread.
    // Assumption is that this should be outside any lock
    HMSQuery q = getHMSQuery();
    q.resetValues(HMSQuery.INIT, null, null, false);
    this.initFuture = hmsQueriesExecutorService.submit(q);
  }

  private static String setDefaultPath(SnappyHiveConf metadataConf,
      ConfVars var, String path) {
    String pathUsed = metadataConf.get(var.varname);
    if (pathUsed == null || pathUsed.isEmpty() ||
        pathUsed.equals(var.getDefaultExpr())) {
      // set the path to provided
      pathUsed = new java.io.File(path).getAbsolutePath();
      metadataConf.setVar(var, pathUsed);
    }
    return pathUsed;
  }

  /**
   * Common connection properties set on metastore JDBC connections.
   */
  public static String getCommonJDBCSuffix() {
    return ";disable-streaming=true;default-persistent=true;" +
        "sync-commits=true;internal-connection=true";
  }

  /**
   * Set the common hive metastore properties and also invoke
   * the static initialization for Hive with system properties
   * which tries booting default derby otherwise (SNAP-1956, SNAP-1961).
   *
   * Should be called after all other properties have been filled in.
   *
   * @return the location of hive warehouse (unused but hive creates the directory)
   */
  public static String initCommonHiveMetaStoreProperties(
      SnappyHiveConf metadataConf) {
    metadataConf.set("datanucleus.mapping.Schema", Misc.SNAPPY_HIVE_METASTORE);
    // Tomcat pool has been shown to work best but does not work in split mode
    // because upstream spark does not ship with it (and the one in snappydata-core
    //   cannot be loaded by datanucleus which should be in system CLASSPATH).
    // Using inbuilt DBCP pool which allows setting the max time to wait
    // for a pool connection else BoneCP hangs if network servers are down, for example,
    // and the thrift JDBC connection fails since its default timeout is infinite.
    // The DBCP 1.x versions are thoroughly outdated and should not be used but
    // the expectation is that the one bundled in datanucleus will be in better shape.
    metadataConf.setVar(ConfVars.METASTORE_CONNECTION_POOLING_TYPE,
        "dbcp-builtin");
    // set the scratch dir inside current working directory (unused but created)
    setDefaultPath(metadataConf, ConfVars.SCRATCHDIR, "./hive");
    // set the warehouse dir inside current working directory (unused but created)
    String warehouse = setDefaultPath(metadataConf,
        ConfVars.METASTOREWAREHOUSE, "./warehouse");
    metadataConf.setVar(ConfVars.HADOOPFS, "file:///");

    metadataConf.set("datanucleus.connectionPool.testSQL", "VALUES(1)");

    // ensure no other Hive instance is alive for this thread but also
    // set the system properties because this can initialize Hive static
    // instance that will try to boot default derby otherwise
    Properties props = metadataConf.getAllProperties();
    Set<String> propertyNames = props.stringPropertyNames();
    for (String name : propertyNames) {
      System.setProperty(name, props.getProperty(name));
    }
    Hive.closeCurrent();

    // set integer properties after the system properties have been used by
    // Hive static initialization so that these never go into system properties

    // every session has own hive client, so a small pool
    // metadataConf.set("datanucleus.connectionPool.maxPoolSize", "4");
    // metadataConf.set("datanucleus.connectionPool.minPoolSize", "0");
    metadataConf.set("datanucleus.connectionPool.maxActive", "4");
    metadataConf.set("datanucleus.connectionPool.maxIdle", "2");
    metadataConf.set("datanucleus.connectionPool.minIdle", "0");
    // throw pool exhausted exception after 30s
    metadataConf.set("datanucleus.connectionPool.maxWait", "30000");

    return warehouse;
  }

  @Override
  public boolean waitForInitialization() {
    // skip for call from within initHMC
    return !Thread.currentThread().getThreadGroup().getName().equals(
        THREAD_GROUP_NAME) && GemFireStore.handleCatalogInit(this.initFuture);
  }

  @Override
  public Table getTable(String schema, String tableName, boolean skipLocks) {
    HMSQuery q = getHMSQuery();
    q.resetValues(HMSQuery.GET_TABLE, tableName, schema, skipLocks);
    Future<Object> f = this.hmsQueriesExecutorService.submit(q);
    return (Table)handleFutureResult(f);
  }

  @Override
  public boolean isColumnTable(String schema, String tableName, boolean skipLocks) {
    HMSQuery q = getHMSQuery();
    q.resetValues(HMSQuery.ISCOLUMNTABLE_QUERY, tableName, schema, skipLocks);
    Future<Object> f = this.hmsQueriesExecutorService.submit(q);
    return (Boolean)handleFutureResult(f);
  }

  @Override
  public boolean isRowTable(String schema, String tableName, boolean skipLocks) {
    HMSQuery q = getHMSQuery();
    q.resetValues(HMSQuery.ISROWTABLE_QUERY, tableName, schema, skipLocks);
    Future<Object> f = this.hmsQueriesExecutorService.submit(q);
    return (Boolean)handleFutureResult(f);
  }

  @Override
  public List<ExternalTableMetaData> getHiveTables(boolean skipLocks) {
    // skip if this is already the catalog lookup thread (Hive dropTable
    //   invokes getTables again)
    if (Boolean.TRUE.equals(HiveTablesVTI.SKIP_HIVE_TABLE_CALLS.get())) {
      return Collections.emptyList();
    }
    HMSQuery q = getHMSQuery();
    q.resetValues(HMSQuery.GET_HIVE_TABLES, null, null, skipLocks);
    Future<Object> f = this.hmsQueriesExecutorService.submit(q);
    // noinspection unchecked
    return (List<ExternalTableMetaData>)handleFutureResult(f);
  }

  @Override
  public String getColumnTableSchemaAsJson(String schema, String tableName,
      boolean skipLocks) {
    HMSQuery q = getHMSQuery();
    q.resetValues(HMSQuery.COLUMNTABLE_SCHEMA, tableName, schema, skipLocks);
    Future<Object> f = this.hmsQueriesExecutorService.submit(q);
    return (String)handleFutureResult(f);
  }

  @Override
  public List<PolicyTableData> getPolicies(boolean skipLocks) {
    // skip if this is already the catalog lookup thread (Hive dropTable
    //   invokes getTables again)
    if (Boolean.TRUE.equals(HiveTablesVTI.SKIP_HIVE_TABLE_CALLS.get())) {
      return Collections.emptyList();
    }
    HMSQuery q = getHMSQuery();
    q.resetValues(HMSQuery.GET_POLICIES, null, null, skipLocks);
    Future<Object> f = this.hmsQueriesExecutorService.submit(q);
    // noinspection unchecked
    return (List<PolicyTableData>)handleFutureResult(f);
  }

  @Override
  public ExternalTableMetaData getHiveTableMetaData(String schema, String tableName,
      boolean skipLocks) {
    HMSQuery q = getHMSQuery();
    q.resetValues(HMSQuery.GET_COL_TABLE, tableName, schema, skipLocks);
    Future<Object> f = this.hmsQueriesExecutorService.submit(q);
    return (ExternalTableMetaData)handleFutureResult(f);
  }

  @Override
  public HashMap<String, List<String>> getAllStoreTablesInCatalog(boolean skipLocks) {
    HMSQuery q = getHMSQuery();
    q.resetValues(HMSQuery.GET_ALL_TABLES_MANAGED_IN_DD, null, null, skipLocks);
    Future<Object> f = this.hmsQueriesExecutorService.submit(q);
    // noinspection unchecked
    return (HashMap<String, List<String>>)handleFutureResult(f);
  }

  @Override
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
  public void close() {
    HMSQuery q = getHMSQuery();
    q.resetValues(HMSQuery.CLOSE_HMC, null, null, true);
    try {
      this.hmsQueriesExecutorService.submit(q).get(5, TimeUnit.SECONDS);
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
    } catch (ExecutionException e) {
      throw new RuntimeException(e.getCause());
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
    private static final int GET_POLICIES = 10;

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
      HiveTablesVTI.SKIP_HIVE_TABLE_CALLS.set(Boolean.TRUE);
      try {
        if (this.skipLock) {
          GfxdDataDictionary.SKIP_LOCKS.set(true);
        }
        return invoke();
      } finally {
        GfxdDataDictionary.SKIP_LOCKS.set(false);
      }
    }

    private Object invoke() throws Exception {
      Hive hmc;
      switch (this.qType) {
        case INIT:
          // Take read/write lock on metastore. Because of this all the servers
          // will initiate their hive client one by one. This is important as we
          // have downgraded the ISOLATION LEVEL from SERIALIZABLE to REPEATABLE READ
          final String hiveClientObject = "HiveMetaStoreClient";
          final GfxdDRWLockService lockService = Misc.getMemStoreBooting()
              .getDDLLockService();
          final GFToSlf4jBridge logger = (GFToSlf4jBridge)Misc.getI18NLogWriter();
          final int previousLevel = logger.getLevel();
          final Logger log4jLogger = LogManager.getRootLogger();
          final Level log4jLevel = log4jLogger.getEffectiveLevel();
          logger.info("Starting hive meta-store initialization");
          // just log the warning messages, during hive client initialization
          // as it generates hundreds of line of logs which are of no use.
          // Once the initialization is done, restore the logging level.
          final boolean reduceLog = previousLevel == LogWriterImpl.CONFIG_LEVEL
              || previousLevel == LogWriterImpl.INFO_LEVEL;
          if (reduceLog) {
            logger.setLevel(LogWriterImpl.WARNING_LEVEL);
            log4jLogger.setLevel(Level.WARN);
          }

          final Object lockOwner = lockService.newCurrentOwner();
          boolean writeLock = false;
          boolean dlockTaken = lockService.lock(hiveClientObject,
              GfxdLockSet.MAX_LOCKWAIT_VAL, -1);
          boolean lockTaken = false;
          try {
            // downgrade dlock to a read lock if hive metastore has already
            // been initialized by some other server
            if (dlockTaken && Misc.getRegionByPath("/" + SystemProperties
                .SNAPPY_HIVE_METASTORE + "/FUNCS", false) != null) {
              lockService.unlock(hiveClientObject);
              dlockTaken = false;
              lockTaken = lockService.readLock(hiveClientObject, lockOwner,
                  GfxdLockSet.MAX_LOCKWAIT_VAL);
              // reduce log4j level to avoid "function exists" warnings
              if (reduceLog) {
                log4jLogger.setLevel(Level.ERROR);
              }
            } else {
              lockTaken = lockService.writeLock(hiveClientObject, lockOwner,
                  GfxdLockSet.MAX_LOCKWAIT_VAL, -1);
              writeLock = true;
            }
            synchronized (hiveClientSync) {
              initHMC();
            }
          } finally {
            if (lockTaken) {
              if (writeLock) {
                lockService.writeUnlock(hiveClientObject, lockOwner);
              } else {
                lockService.readUnlock(hiveClientObject);
              }
            }
            if (dlockTaken) {
              lockService.unlock(hiveClientObject);
            }
            logger.setLevel(previousLevel);
            log4jLogger.setLevel(log4jLevel);
            logger.info("Done hive meta-store initialization");
          }
          return true;

        case ISROWTABLE_QUERY:
          hmc = Hive.get();
          String type = getType(hmc);
          return type.equalsIgnoreCase(ExternalTableType.Row().name());

        case ISCOLUMNTABLE_QUERY:
          hmc = Hive.get();
          type = getType(hmc);
          return !type.equalsIgnoreCase(ExternalTableType.Row().name());

        case COLUMNTABLE_SCHEMA:
          hmc = Hive.get();
          return getSchema(hmc);

        case GET_TABLE:
          hmc = Hive.get();
          return getTable(hmc, this.dbName, this.tableName);

        case GET_HIVE_TABLES: {
          hmc = Hive.get();
          List<String> schemas = hmc.getAllDatabases();
          ArrayList<ExternalTableMetaData> externalTables = new ArrayList<>();
          for (String schema : schemas) {
            List<String> tables = hmc.getAllTables(schema);
            for (String tableName : tables) {
              try {
                Table table = hmc.getTable(schema, tableName);
                Properties metadata = table.getMetadata();
                String tblDataSourcePath = metadata.getProperty("path");
                tblDataSourcePath = tblDataSourcePath == null ? "" : tblDataSourcePath;
                String driverClass = metadata.getProperty("driver");
                driverClass = ((driverClass == null) || driverClass.isEmpty()) ? "" : driverClass;
                String tableType = ExternalTableType.getTableType(table);
                // exclude policies also from the list of hive tables
                if (!(ExternalTableType.Row().name().equalsIgnoreCase(tableType)
                    || ExternalTableType.Policy().name().equalsIgnoreCase(tableType))) {
                  // TODO: FIX ME: should not convert to upper case blindly
                  // but unfortunately hive meta-store is not case-sensitive
                  ExternalTableMetaData metaData = new ExternalTableMetaData(
                      Utils.toUpperCase(table.getTableName()),
                      Utils.toUpperCase(table.getDbName()),
                      tableType, null, -1, -1,
                      null, null, null, null,
                      tblDataSourcePath, driverClass);
                  metaData.provider = table.getParameters().get(
                      SnappyStoreHiveCatalog.HIVE_PROVIDER());
                  metaData.shortProvider = SnappyContext.getProviderShortName(metaData.provider);
                  metaData.columns = ExternalStoreUtils.getColumnMetadata(
                      ExternalStoreUtils.getTableSchema(table));
                  if ("VIEW".equalsIgnoreCase(tableType)) {
                    metaData.viewText = SnappyStoreHiveCatalog
                        .getViewTextFromHiveTable(table);
                  }
                  externalTables.add(metaData);
                }
              } catch (Exception e) {
                // ignore exception and move to next
                Misc.getI18NLogWriter().warning(LocalizedStrings.DEBUG,
                    "Failed to retrieve information for " + tableName + ": " + e);
              }
            }
          }
          return externalTables;
        }
        case GET_POLICIES: {
          hmc = Hive.get();
          List<String> schemas = hmc.getAllDatabases();
          ArrayList<PolicyTableData> policyData = new ArrayList<>();
          for (String schema : schemas) {
            List<String> tables = hmc.getAllTables(schema);
            for (String tableName : tables) {
              try {
                Table table = hmc.getTable(schema, tableName);
                Properties metadata = table.getMetadata();

                String tableType = ExternalTableType.getTableType(table);
                // exclude policies also from the list of hive tables
                if (ExternalTableType.Policy().name().equalsIgnoreCase(tableType)) {
                  String policyFor = Utils.toUpperCase(
                      metadata.getProperty(PolicyProperties.policyFor()));
                  String policyApplyTo = Utils.toUpperCase(
                      metadata.getProperty(PolicyProperties.policyApplyTo()));
                  String targetTable = Utils.toUpperCase(
                      metadata.getProperty(PolicyProperties.targetTable()));
                  String filter = Utils.toUpperCase(
                      metadata.getProperty(PolicyProperties.filterString()));
                  String owner = Utils.toUpperCase(
                      metadata.getProperty(PolicyProperties.policyOwner()));
                  PolicyTableData metaData = new PolicyTableData(
                      Utils.toUpperCase(table.getTableName()),
                      policyFor, policyApplyTo, targetTable, filter, owner);
                  metaData.columns = ExternalStoreUtils.getColumnMetadata(
                      ExternalStoreUtils.getTableSchema(table));
                  policyData.add(metaData);
                }
              } catch (Exception e) {
                // ignore exception and move to next
                Misc.getI18NLogWriter().warning(LocalizedStrings.DEBUG,
                    "Failed to retrieve information for " + tableName + ": " + e);
              }
            }
          }
          return policyData;
        }

        case GET_ALL_TABLES_MANAGED_IN_DD:
          hmc = Hive.get();
          List<String> dbList = hmc.getAllDatabases();
          HashMap<String, List<String>> dbTablesMap = new HashMap<>();
          for (String db : dbList) {
            List<String> tables = hmc.getAllTables(db);
            // TODO: FIX ME: should not convert to upper case blindly
            List<String> upperCaseTableNames = new LinkedList<>();
            for (String t : tables) {
              Table hiveTab = hmc.getTable(db, t);
              String tableType = ExternalTableType.getTableType(hiveTab);
              if (ExternalTableType.isTableBackedByRegion(tableType)) {
                upperCaseTableNames.add(Utils.toUpperCase(t));
              }
            }
            dbTablesMap.put(Utils.toUpperCase(db), upperCaseTableNames);
          }
          return dbTablesMap;
        case REMOVE_TABLE:
          hmc = Hive.get();
          hmc.dropTable(this.dbName, this.tableName);
          return true;
        case GET_COL_TABLE:
          hmc = Hive.get();
          Table table = getTableWithRetry(hmc);
          if (table == null) return null;
          String fullyQualifiedName = Utils.toUpperCase(table.getDbName()) +
              "." + Utils.toUpperCase(table.getTableName());
          StructType schema = ExternalStoreUtils.getTableSchema(table);
          @SuppressWarnings("unchecked")
          Map<String, String> parameters = new CaseInsensitiveMap(
              table.getSd().getSerdeInfo().getParameters());
          String parts = parameters.get(ExternalStoreUtils.BUCKETS());
          // get the partitions from the actual table if not in catalog
          int partitions;
          if (parts != null) {
            partitions = Integer.parseInt(parts);
          } else {
            PartitionAttributes pattrs = Misc.getRegionForTableByPath(
                fullyQualifiedName, true)
                .getAttributes().getPartitionAttributes();
            partitions = pattrs != null ? pattrs.getTotalNumBuckets() : 1;
          }
          Object value = parameters.get(StoreUtils.GEM_INDEXED_TABLE());
          String baseTable = value != null ? value.toString() : "";
          String dmls = JdbcExtendedUtils.
              getInsertOrPutString(fullyQualifiedName, schema, false, false);
          value = parameters.get(ExternalStoreUtils.DEPENDENT_RELATIONS());
          String[] dependentRelations = value != null
              ? value.toString().split(",") : null;
          int columnBatchSize = ExternalStoreUtils.sizeAsBytes(parameters.get(
              ExternalStoreUtils.COLUMN_BATCH_SIZE()), ExternalStoreUtils.COLUMN_BATCH_SIZE());
          int columnMaxDeltaRows = ExternalStoreUtils.checkPositiveNum(Integer.parseInt(
              parameters.get(ExternalStoreUtils.COLUMN_MAX_DELTA_ROWS())),
              ExternalStoreUtils.COLUMN_MAX_DELTA_ROWS());
          value = parameters.get(ExternalStoreUtils.COMPRESSION_CODEC());
          String compressionCodec = value == null ? Constant.DEFAULT_CODEC() : value.toString();
          String tableType = ExternalTableType.getTableType(table);
          String tblDataSourcePath = table.getMetadata().getProperty("path");
          tblDataSourcePath = tblDataSourcePath == null ? "" : tblDataSourcePath;
          String driverClass = table.getMetadata().getProperty("driver");
          driverClass = ((driverClass == null) || driverClass.isEmpty()) ? "" : driverClass;
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
              dependentRelations,
              tblDataSourcePath,
              driverClass);

        case CLOSE_HMC:
          Hive.closeCurrent();
          return true;

        default:
          throw new IllegalStateException("HiveMetaStoreClient:unknown query option");
      }
    }

    public String toString() {
      return "HiveMetaStoreQuery:query type = " + this.qType + " tname = " +
          this.tableName + " db = " + this.dbName;
    }

    private void initHMC() {
      ExternalStoreUtils.registerBuiltinDrivers();

      SnappyHiveConf metadataConf = new SnappyHiveConf();
      String urlSecure = "jdbc:snappydata:" +
          ";user=" + SnappyStoreHiveCatalog.HIVE_METASTORE() +
          getCommonJDBCSuffix();
      final Map<Object, Object> bootProperties = Misc.getMemStore().getBootProperties();
      if (bootProperties.containsKey(Attribute.USERNAME_ATTR) && bootProperties.containsKey
          (Attribute.PASSWORD_ATTR)) {
        urlSecure = "jdbc:snappydata:" +
            ";user=" + bootProperties.get(Attribute.USERNAME_ATTR) +
            ";password=" + bootProperties.get(Attribute.PASSWORD_ATTR) +
            ";default-schema=" + SnappyStoreHiveCatalog.HIVE_METASTORE() +
            getCommonJDBCSuffix();
        /*
        metadataConf.setVar(ConfVars.METASTORE_CONNECTION_USER_NAME,
            bootProperties.get("user").toString());
        metadataConf.setVar(ConfVars.METASTOREPWD,
            bootProperties.get("password").toString());
        */
      } else {
        metadataConf.setVar(ConfVars.METASTORE_CONNECTION_USER_NAME,
            Misc.SNAPPY_HIVE_METASTORE);
      }
      metadataConf.setVar(ConfVars.METASTORECONNECTURLKEY, urlSecure);
      metadataConf.setVar(ConfVars.METASTORE_CONNECTION_DRIVER,
          Constant.JDBC_EMBEDDED_DRIVER());
      initCommonHiveMetaStoreProperties(metadataConf);

      final short numRetries = 40;
      short count = 0;
      while (true) {
        try {
          Hive hmc = Hive.get(metadataConf);
          // a dummy table query to pre-initialize most of hive metastore tables
          try {
            getTable(hmc, "APP", "DUMMY");
          } catch (SQLException ignored) {
          }
          break;
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

    private Table getTable(Hive hmc, String dbName, String tableName) throws SQLException {
      try {
        return hmc.getTable(dbName, tableName);
      } catch (InvalidTableException ignored) {
        return null;
      } catch (HiveException he) {
        throw Util.generateCsSQLException(SQLState.TABLE_NOT_FOUND,
            tableName, he);
      }
    }

    private String getType(Hive hmc) throws SQLException {
      return ExternalTableType.getTableType(getTable(hmc, this.dbName, this.tableName));
    }

    private Table getTableWithRetry(Hive hmc) throws SQLException {
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

    private String getSchema(Hive hmc) throws SQLException {
      Table table = getTableWithRetry(hmc);
      if (table != null) {
        return SnappyStoreHiveCatalog.getSchemaStringFromHiveTable(table);
      } else {
        return null;
      }
    }
  }
}
