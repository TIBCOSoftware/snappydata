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
package org.apache.spark.sql.internal;

import com.pivotal.gemfirexd.internal.engine.Misc;
import io.snappydata.sql.catalog.SnappyExternalCatalog;
import io.snappydata.sql.catalog.impl.SmartConnectorExternalCatalog;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.ClusterMode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SnappyContext;
import org.apache.spark.sql.SnappyEmbeddedMode;
import org.apache.spark.sql.SnappySession;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.ThinClientConnectorMode;
import org.apache.spark.sql.catalyst.catalog.ExternalCatalog;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.CacheManager;
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils;
import org.apache.spark.sql.execution.ui.SQLListener;
import org.apache.spark.sql.execution.ui.SQLTab;
import org.apache.spark.sql.execution.ui.SnappySQLListener;
import org.apache.spark.sql.hive.HiveClientUtil$;
import org.apache.spark.sql.hive.SnappyHiveExternalCatalog;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.ui.SparkUI;

/**
 * Overrides Spark's SharedState to enable setting up own ExternalCatalog.
 * Implemented in java to enable overriding "val externalCatalog" with a different
 * class object but as a function rather than a "val" allowing to return
 * super.externalCatalog temporarily when it gets invoked in super's constructor.
 */
public final class SnappySharedState extends SharedState {

  /**
   * Instance of {@link SnappyCacheManager} to enable clearing cached plans.
   */
  private final CacheManager snappyCacheManager;

  /**
   * The ExternalCatalog implementation used for SnappyData in embedded mode.
   */
  private final SnappyHiveExternalCatalog embedCatalog;

  /**
   * Used to skip initializing meta-store in super's constructor.
   */
  private final boolean initialized;

  private static final String CATALOG_IMPLEMENTATION =
      StaticSQLConf.CATALOG_IMPLEMENTATION().key();

  private static final String WAREHOUSE_DIR =
      StaticSQLConf.WAREHOUSE_PATH().key();

  /**
   * Simple extension to CacheManager to enable clearing cached plan on cache create/drop.
   */
  private static final class SnappyCacheManager extends CacheManager {

    @Override
    public void cacheQuery(Dataset<?> query, scala.Option<String> tableName,
        StorageLevel storageLevel) {
      super.cacheQuery(query, tableName, storageLevel);
      // clear plan cache since cached representation can change existing plans
      ((SnappySession)query.sparkSession()).clearPlanCache();
    }

    @Override
    public void uncacheQuery(SparkSession session, LogicalPlan plan, boolean blocking) {
      super.uncacheQuery(session, plan, blocking);
      // clear plan cache since cached representation can change existing plans
      ((SnappySession)session).clearPlanCache();
    }

    @Override
    public void recacheByPlan(SparkSession session, LogicalPlan plan) {
      super.recacheByPlan(session, plan);
      // clear plan cache since cached representation can change existing plans
      ((SnappySession)session).clearPlanCache();
    }

    public void recacheByPath(SparkSession session, String resourcePath) {
      super.recacheByPath(session, resourcePath);
      // clear plan cache since cached representation can change existing plans
      ((SnappySession)session).clearPlanCache();
    }
  }

  /**
   * Create Snappy's SQL Listener instead of SQLListener
   */
  private static void createListenerAndUI(SparkContext sc) {
    SQLListener initListener = ExternalStoreUtils.getSQLListener().get();
    if (initListener == null) {
      SnappySQLListener listener = new SnappySQLListener(sc.conf());
      if (ExternalStoreUtils.getSQLListener().compareAndSet(null, listener)) {
        sc.addSparkListener(listener);
        scala.Option<SparkUI> ui = sc.ui();
        // embedded mode attaches SQLTab later via ToolsCallbackImpl that also
        // takes care of injecting any authentication module if configured
        if (ui.isDefined() &&
            !(SnappyContext.getClusterMode(sc) instanceof SnappyEmbeddedMode)) {
          new SQLTab(listener, ui.get());
        }
      }
    }
  }

  private SnappySharedState(SparkContext sparkContext) {
    super(sparkContext);

    // avoid inheritance of activeSession
    SparkSession.clearActiveSession();

    this.snappyCacheManager = new SnappyCacheManager();
    ClusterMode clusterMode = SnappyContext.getClusterMode(sparkContext);
    if (clusterMode instanceof ThinClientConnectorMode) {
      this.embedCatalog = null;
    } else {
      // ensure store catalog is initialized
      Misc.getMemStoreBooting().getExistingExternalCatalog();
      this.embedCatalog = HiveClientUtil$.MODULE$.getOrCreateExternalCatalog(
          sparkContext, sparkContext.conf());
    }

    this.initialized = true;
  }

  public static synchronized SnappySharedState create(SparkContext sparkContext) {
    // force in-memory catalog to avoid initializing external hive catalog at this point
    final String catalogImpl = sparkContext.conf().get(CATALOG_IMPLEMENTATION, null);
    final String warehouseDir = sparkContext.conf().get(WAREHOUSE_DIR, null);
    // there is a small thread-safety issue in that if multiple threads
    // are initializing normal concurrently SparkSession vs SnappySession
    // then former can land up with in-memory catalog too
    sparkContext.conf().set(CATALOG_IMPLEMENTATION, "in-memory");
    // always use default local path for warehouse dir (not used by SD but required by hive client)
    sparkContext.conf().set(WAREHOUSE_DIR, StaticSQLConf.WAREHOUSE_PATH().defaultValueString());

    createListenerAndUI(sparkContext);

    final SnappySharedState sharedState = new SnappySharedState(sparkContext);

    // reset the temporary confs to original
    if (catalogImpl != null) {
      sparkContext.conf().set(CATALOG_IMPLEMENTATION, catalogImpl);
    } else {
      sparkContext.conf().remove(CATALOG_IMPLEMENTATION);
    }
    if (warehouseDir != null) {
      sparkContext.conf().set(WAREHOUSE_DIR, warehouseDir);
    } else {
      sparkContext.conf().remove(WAREHOUSE_DIR);
    }
    return sharedState;
  }

  /**
   * Returns the global external hive catalog embedded mode, while in smart
   * connector mode returns a new instance of external catalog since it
   * may need credentials of the current user to be able to make meta-data
   * changes or even to read it.
   */
  public SnappyExternalCatalog getExternalCatalogInstance(SnappySession session) {
    if (!this.initialized) {
      throw new IllegalStateException("getExternalCatalogInstance: unexpected invocation " +
          "from within SnappySharedState constructor");
    } else if (this.embedCatalog != null) {
      return this.embedCatalog;
    } else {
      // create a new connector catalog instance for connector mode
      // each instance has its own set of credentials for authentication
      return new SmartConnectorExternalCatalog(session);
    }
  }

  @Override
  public CacheManager cacheManager() {
    if (this.initialized) {
      return this.snappyCacheManager;
    } else {
      // in super constructor, no harm in returning super's value at this point
      return super.cacheManager();
    }
  }

  @Override
  public ExternalCatalog externalCatalog() {
    if (this.initialized) {
      return this.embedCatalog;
    } else {
      // in super constructor, no harm in returning super's value at this point
      return super.externalCatalog();
    }
  }
}
